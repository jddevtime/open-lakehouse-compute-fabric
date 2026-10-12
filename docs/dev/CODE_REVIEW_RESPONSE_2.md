# Response to the release-readiness review of 9 October 2026

Author: Justin Dodrill. Written 9 October 2026, for [`CODE_REVIEW_2026-10-09.md`](CODE_REVIEW_2026-10-09.md) (the second external review, "Release Readiness, Changes and Recommendations"). The first review and its answer are [`CODE_REVIEW_2026-10-08.md`](CODE_REVIEW_2026-10-08.md) and [`CODE_REVIEW_RESPONSE.md`](CODE_REVIEW_RESPONSE.md); that page stays as historical evidence.

The reviewer worked on a separate extracted copy of the project and edited it in place. That copy was not available to us, so **we did not merge the reviewer's files: we re-implemented every applied change (C01 to C14) in this tree from the report's description, writing the regression test first and watching it fail on the old code where the defect could be reproduced.** Where our implementation differs from the report, or a claim did not reproduce, this page says so. The recommendations (R01 to R14) are *not* code: they are live acceptance work, tracked in [`docs/development/release-readiness.md`](../development/release-readiness.md), and **none of them is marked done**.

As before, everything here was exercised on Linux with test doubles.

## Scorecard of the applied changes

| ID | Priority | Reproduced here | Implemented | Where it differs from the report |
|---|---|---|---|---|
| C01 Spark function allowlist, no unparsed writer fallback | P1 | yes (67 new tests failed on the old code) | yes | We also reject `DROP FUNCTION` (a writer could otherwise drop a registered function); typed sqlglot nodes are accepted by their rendered Spark name; `xpath*` is deliberately **not** allowlisted |
| C02 Livy POST 503 is uncertain | P1 | yes | yes | Stricter than the report: *every* 5xx answer to a POST (500, 502, 503, 504) is `SubmissionUncertain` after exactly one POST; only 429 is a known rejection |
| C03 HTTP activity 503 is uncertain | P1 | yes | yes | `NOT_PROCESSED_STATUSES = (408, 425, 429)`. In this code only GET/HEAD/OPTIONS are "safe"; PUT/DELETE retry only with an idempotency key or `policy.idempotent` (unchanged) |
| C04 Socket owner closes an aborted stream | P1 | yes (old code: reader got `EBADF` instead of EOF; the new descriptor-ownership test failed) | yes | Five real `socketpair` tests; 8 consecutive stress repetitions passed |
| C05 Unsafe runtime trust fails readiness | P1 | yes | yes | See below: we also found the Helm readiness probe never called the ready endpoint, and the docs named a liveness route that does not exist |
| C06 Source-archive gate, both distributions | P2 | yes (`rel0` build failed at the gate; `.hypothesis/` was also being shipped) | yes | `license-files` is set under `[tool.setuptools]` for the SDK, because `project.license-files` would force a setuptools bump and drop a classifier |
| C07 `UNKNOWN` license field falls through to classifiers | P2 | yes (ptyprocess shape) | yes | `usable_license_field` / `resolve_license`; unidentified licenses still fail |
| C08 Release build needs the three gates | P2 | by reading | yes | `actionlint` 1.7.12 passes on all workflows; **never executed on GitHub** |
| C09 Draft releases, prerelease flags, separate PyPI workflow | P2 | by reading | yes | Same caveat; the `images` job uses a protected `release` environment because a tag push otherwise publishes versioned GHCR images before the draft is approved |
| C10 Release identity preflight and private contact | P2 | yes | yes | The preflight scans the whole tree, so it also reports `CHANGELOG.md` and `docs/development/setup.md`. **It fails today, on purpose** |
| C11 Secret scanning without hiding tests | P2 | yes (gitleaks 8.24.2: 27 findings before, none after) | yes | 20 fixture lines annotated across the 12 files; one extra (`tests/security/test_leaks.py:249`). `[allowlist]` is used because `[[allowlists]]` was not honoured by 8.24.2 |
| C12 Documentation matches the evidence | P2 | n/a | yes | README claims narrowed, test counts removed, `release-readiness.md` and `release-process.md` added or rewritten |
| C13 Credential regression scoped to its workspace | P2 | **mechanism only** | yes | The natural flake did not occur (3 of 3 full-file runs passed before the change); a scratch test that submitted another workspace's job first did fail with 3 submissions instead of 2. The fix is the report's scoping |
| C14 Node TAP reporter | P2 | **no** | yes | Node 22 already printed the TAP summary when captured here; the explicit `--test-reporter=tap` is a portability fix, not a repaired failure |

## Things we found beyond the report

* **The Helm readiness probe was not wired to the ready endpoint** (C05). All three probes ran `olcf-healthcheck`, which calls `/v1/health`, so Kubernetes would never have seen the new 503. `deploy/docker/healthcheck.py` now takes an optional path (only `/v1/health*` is accepted) and the chart's readiness probe passes `/v1/health/ready`. The Docker `HEALTHCHECK` still checks liveness.
* **There is no `/v1/health/live` route**; liveness is `/v1/health`. The documentation said otherwise and was corrected rather than adding an alias.
* **Our own text on job credentials was wrong in `SECURITY.md` and `docs/concepts/security-model.md`**: they said the token of a failed launch stays valid until it expires. The code records the token id on the attempt before the launch is built and revokes it on launch failure (pinned by an existing test). Both were corrected.
* **`.hypothesis/` was being shipped in the source archive**; it is now ignored and forbidden by the gate.
* **Git history contains the synthetic fixtures.** A history scan (`gitleaks git`) reports 24 findings in older commits: they are the same test fixtures and documentation examples that the tree scan now annotates or has replaced (JWT-shaped strings, AWS-key-shaped strings, one throw-away private key, generated example passwords from a sandbox quickstart). None is a credential for a real system. The release workflow runs the history scan as informational and the tree scan as blocking. If you want a spotless history for the first public push, publish from a fresh single-commit history instead of this one.

## What changed in behaviour

* Spark SQL requests, notebook cells excluded, now fail closed for **any function not on the allowlist** (`src/olcf/sql/spark_functions.py`, 446 names), for qualified functions, and for text that does not parse into exactly one supported statement, for every role. Custom UDFs are therefore refused in SQL requests; run them in a notebook or job. A genuine built-in we missed is refused until it is added with a positive and a negative test.
* A submission that may have been accepted (HTTP 5xx on a Livy POST, 503 on an unsafe pipeline HTTP call) is never replayed; it is reconciled (Livy) or surfaced as `retrySuppressed` (pipelines).
* In production, trusting the public development signing key makes `/v1/health/ready` return 503 (`{"status":"unavailable"}`, no detail). Development mode, quickstart and production with operator anchors stay ready; production with no anchor at all is only `degraded`.
* Release builds are drafts, 0.x tags are prereleases and never move `latest`, and the release build requires the CI, container and security workflows to pass.

## Verification

The full gate results are recorded in the CHANGELOG entry for this follow-up and in `docs/dev/CONTRACTS.md` (the C01/C02/C03/C04/C05 notes). Per-engineer results: `tests/data` 908 passed; `tests/orch` 398 passed; `tests/security` 383 passed; `tests/tooling` 383 passed; `tests/e2e` 55 passed; the release build (`bash scripts/build_release.sh`) builds both wheels and sdists, installs both wheels into clean environments, writes both SBOMs and a verified `SHA256SUMS`.

Known test flakiness under heavy parallel load (observed when several engineers ran suites at once, passing alone and in a clean run): `test_recovery.py::test_callback_received_while_the_dispatcher_was_down_is_applied`, `test_jobs_flow.py::test_redelivered_job_events_are_deduplicated_by_seq`, `test_sql_session_guard.py::test_session_commands_never_reach_a_kernel_whoever_asks`, and two `test_embedded.py` cases that need network access for `uv`. They are listed so that a failure on CI is not a surprise.

## What is still not done

1. **R01, the blocker.** The real GitHub owner and repository have not been given and are not invented. `python scripts/check_release_config.py` fails until they are applied (it lists the files). The contact address (jdsql@hotmail.com) is set for conduct and security reports; private vulnerability reporting, branch protection and required checks have to be enabled on GitHub.
2. **R02.** No workflow has run. A `workflow_dispatch` dry run on the real repository is the next gate, then both PyPI trusted-publisher registrations (`olcf`, `olcf-fabric`) if PyPI publication is wanted. GitHub's prerelease flag does not make PyPI `0.1.0` a prerelease; align PEP 440 versions before publishing a PyPI beta.
3. **R03 to R10, R12.** Private runtime signing, real isolation between users' code (until then: trusted-user beta compute), a cluster with an enforcing CNI, a Livy gateway, a Fabric/Entra/OneLake tenant, clean Windows/macOS/systemd hosts, session/kernel recovery after a restore, multi-node PostgreSQL with mTLS under failure, and a Spark-version query corpus for the allowlist.
4. **R11, R13, R14.** Pin third-party actions by commit SHA, audit vendored JavaScript and runtime manifests, keep the README and release notes aligned with the scope list, and test the published one-liners on clean hosts once real URLs exist.

The honest release statement is the one the reviewer gave: a clearly scoped **beta for evaluation on trusted infrastructure**, with Kubernetes, Livy, Fabric integration, Windows and multi-tenant isolation described as experimental until their acceptance tests pass.
