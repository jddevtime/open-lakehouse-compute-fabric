# Security policy

OLCF runs user code (notebooks are inherently privileged compute), stores credentials and talks to cloud tenants, so we take reports seriously.

## Reporting a vulnerability

**Do not open a public issue for a security problem.** Report it privately to:

> jdsql@hotmail.com

If the repository is hosted on GitHub, the maintainer enables *private vulnerability reporting* (Settings > Code security) so that you can use *Security > Report a vulnerability* instead; check that the *Report a vulnerability* button is present before relying on it, and use the e-mail address above if it is not.

Please include: the affected version (`olcf version`), how you installed it, what you did, what you expected, what happened, and the smallest
reproduction you can give. Do not include real credentials or customer data. A support bundle made with `olcf diagnostics` is redacted by default
and is welcome, but check it before you send it.

## What happens next

These are the targets the maintainer commits to; they are written for a project with a single maintainer and may be tightened once there are more.

| Step | Target |
|---|---|
| Acknowledgement of your report | within 5 business days |
| First assessment (accepted, needs more information, or not a vulnerability) | within 10 business days |
| Fix or mitigation for a confirmed high or critical issue | within 30 days; other severities within 90 days |
| Coordinated public disclosure | when a fix is released, or 90 days after the report, whichever is first; earlier if the issue is already being exploited, later only by agreement |
| Credit | in the advisory and the changelog, unless you ask not to be named |

We will not take legal action against people who research and report in good faith, stay within their own installation or test tenant, do not access
other people's data, and give us the time above.

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x (current beta) | yes: security fixes are released as 0.1.y |
| anything older, or unreleased branches | no |

Until a 1.0 release, only the newest 0.x line receives fixes. Upgrade with `olcf upgrade` (see the [runbook](docs/operations/runbook.md#updates)); a database backup is taken first.

## Scope

In scope: the control plane (API, web console, worker hub), the worker agent, the runtime manager and runtime libraries, the installers and
deployment files in this repository, the Python SDKs, the MCP endpoint, and the documentation where it would lead a reader to an insecure setup.

Out of scope: vulnerabilities in third-party software that OLCF merely depends on (report those upstream; tell us if OLCF needs to pin or patch),
findings that need an administrator to attack their own installation, denial of service by authenticated administrators, and issues that only exist in
deliberately insecure configurations that the documentation warns about (for example `--insecure` MCP bridge, `workers.insecureInCluster`, an empty
`server.trusted_proxies` behind a proxy).

## Known limitations that are *not* vulnerabilities

The design decisions and the open items below are documented rather than hidden; the full list is in
[`docs/compliance/requirements-traceability.md`](docs/compliance/requirements-traceability.md#known-limitations).

* Notebook and job code is arbitrary code execution on a worker. Isolation is the worker (process user, scrubbed environment, optional sandbox hook), not a language sandbox; there is no per-pool OS or container isolation. The callback token a job receives is derived from, and capped by, the credential it was submitted with, in one workspace, and ends with its attempt; the code can still read the job's own input file (token, resolved secrets) and whatever the worker's OS user can read.
* Organisation-scoped SecurityAdmin, RuntimeAdmin and Operator roles act across all workspaces by design.
* The login rate limiter is in memory and per node (its table is bounded, so flooding it with distinct keys cannot exhaust memory, but the effective limit is the limit times the number of nodes).
* Logout revokes the whole login (its access and refresh tokens) on every node that shares the database, within about two seconds, and ends the callbacks of jobs and kernels that were submitted with it (use a service token for long-running work). A password change revokes refresh tokens but not access tokens that were already issued, which stay valid until they expire (30 minutes by default).
* The forced password change (`must_change_password`) restricts local login sessions only; personal access tokens, worker tokens and federated identities are not subject to it.
* Master-key and JWT-signing-key rotation (`olcf security rotate`) and worker certificate renewal (`olcf worker renew`) exist. Worker tokens do not expire unless `compute.worker_token_max_age_days` is set.
* Spark RPC authentication and encryption between master, drivers and executors are not configured by OLCF; keep the Spark ports (see [ports](docs/reference/ports-and-networking.md)) on a trusted network. The Helm NetworkPolicy restricts them to the release's pods; it takes effect only on a cluster whose CNI enforces NetworkPolicy.
* A job token's id is recorded on its attempt row before the launch is built or handed to a provider (pre-submission credential registration), so a launch that fails, is refused or is re-queued (for example `WorkerOffline`) has its token revoked at once; a token that did reach a worker is revoked when its attempt ends. A token is also refused as soon as its attempt is no longer live, and in the worst case (a crash between minting and recording) it expires with the job timeout plus one hour.
* The bundled runtime-signing key is a public development key. A production installation (`environment` unset or `production`) does not trust it, so the shipped runtime manifests must be re-signed with your own key (`python -m olcf.runtime keygen`, `trust`, `sign`) before they install; the `runtime-trust` health check is critical: a production installation that trusts the development key (an explicit `runtime.trust.allow_development_key = true` or `OLCF_RUNTIME_ALLOW_DEV_KEY=1`) reports `down` and fails readiness (`GET /v1/health/ready` answers `503` with a fixed body that names no reason; liveness stays `200`). The process still starts, so you can read the reason in `olcf health` / `GET /v1/admin/health` and fix it. Development and `olcf quickstart` installations (`environment = "development"`) and production installations with their own trust anchor stay ready.
* Reports of the 8 October 2026 external code review and their status: [`docs/dev/CODE_REVIEW_RESPONSE.md`](docs/dev/CODE_REVIEW_RESPONSE.md).
* The release-readiness review of 9 October 2026 and its status: [`docs/dev/CODE_REVIEW_RESPONSE_2.md`](docs/dev/CODE_REVIEW_RESPONSE_2.md).

## Hardening

See the [security model](docs/concepts/security-model.md), the [hardening guide](docs/operations/hardening.md) and the
[production checklist](docs/operations/production-checklist.md).
