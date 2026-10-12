<!-- Converted from Open_Lakehouse_Release_Readiness_Changes_and_Recommendations.docx (external review, 9 October 2026). Local file links were reduced to plain paths. The text is the reviewer's, unedited. -->

Prepared for Justin Dodrill by Codex • 9 October 2026

# Release decision

The latest project contains substantial remediation work, and this follow-up applied additional fixes to SQL isolation, uncertain request retries, HTTP cancellation, runtime readiness and release packaging. The local build succeeds and the focused regression checks pass. It is not yet ready for an unqualified production release. Prepare the first public release as a clearly scoped beta after completing repository identity and the actual GitHub validation workflow.

This document is the compiled record of what changed, why it changed, what was tested, and what still needs work. Items C01 through C14 are applied changes. Items R01 through R14 are recommendations and acceptance criteria; they must not be read as completed implementation.

**Current contact** jdsql@hotmail.com is now configured for private conduct and vulnerability reports. No message was sent to that address.

**Source snapshot** /Users/aimac/Desktop/\_projects/Open_Lakehouse_Fabric_Compute_Extender_01zipextracted

**Change inventory** 50 source, test, configuration or documentation files differ from the saved start-of-task baseline. Generated caches and test output are excluded. The appendix identifies every changed file and its purpose.

**Publication status** No GitHub repository, commit, pull request, tag, release, registry image or PyPI package was published in this review. This extracted folder has no Git history to audit.

**Evidence limits** This is a broad risk-based review and local validation pass. Passing local tests, schema checks and a package build do not establish correctness on Microsoft services, Kubernetes/Livy deployments, every operating system, or hostile multi-tenant infrastructure.

# How to use this report

Start with the validation record and remaining release blockers. Review the applied changes before accepting the edited folder. Use the recommendation register to create work items and the final release sequence as a checklist. The historical 8 October review and the supplied response remain separate records; the status matrix here explains how this follow-up relates to them.

# Validation record

| **Check**                      | **Result and scope**                                                                                                                                                                                                                                                                                                                          |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Broad Python run               | 3,086 passed, 3 failed, 18 skipped and 112 deselected in 38 minutes 14 seconds. All three failures were subsequently corrected: credential-test isolation (C13), the old readiness expectation (C05), and Node reporter format (C14). The affected-file rerun passed 21 tests. A complete broad run was not repeated after those corrections. |
| Focused guard and provider run | 265 passed, 4 failed initially. SQL/session and Livy cases passed; four HTTP timeout cases exposed the cancellation race subsequently fixed.                                                                                                                                                                                                  |
| Final targeted regression      | 57 passed: timeout overlap, runtime trust and release artifact tests. These overlap earlier runs and must not be added as unique test coverage.                                                                                                                                                                                               |
| Credential test isolation      | Three consecutive full-file runs passed after the workspace-isolation correction: 13 tests each (39 executions, not 39 unique tests).                                                                                                                                                                                                         |
| Socket ownership regressions   | 2 passed using real socket pairs; the blocked-read stress case passed 8 consecutive fresh-process repetitions.                                                                                                                                                                                                                                |
| Deployment tooling             | 107 passed, 5 skipped using real Helm 3.16.2 and kubeconform 0.6.7. Docker/systemd execution unavailable locally.                                                                                                                                                                                                                             |
| Release build                  | Server and Fabric SDK wheels and source archives built; clean server wheel installation and initialization, CycloneDX SBOM, notices and checksum verification passed.                                                                                                                                                                         |
| Lint and types                 | Ruff passed. Required mypy scope passed on 30 CLI/SDK source files; this is not a whole-codebase type-clean claim.                                                                                                                                                                                                                            |
| Documentation                  | Links, CLI, settings, states and generated references passed after health-check regeneration. Python header check passed on 618 files; required module/class docstring thresholds passed.                                                                                                                                                     |
| Workflow syntax                | Actionlint 1.7.7 passed. Actual GitHub runs, external publication, permissions and trusted-publisher setup remain untested.                                                                                                                                                                                                                   |
| Python advisory audit          | No known vulnerabilities reported for the resolved all-extras runtime dependency set on macOS arm64/Python 3.13. Other platforms and vendored/runtime components need separate coverage.                                                                                                                                                      |
| Secret scan                    | Gitleaks 8.24.3 directory scan passed after narrow synthetic-fixture/public-development-key exceptions. No Git history was available.                                                                                                                                                                                                         |
| Release identity preflight     | Intentionally fails on unresolved GitHub metadata in five public files. This remains a release blocker.                                                                                                                                                                                                                                       |

**Skipped coverage** The broad run skipped 6 cached-Spark-runtime cases, 3 Helm/kubeconform cases, 4 Docker cases, 2 systemd cases, 2 shellcheck cases and 1 real-Windows-lock case. The 3 Helm/kubeconform cases were covered by the separate deployment run with those tools available. The 112 deselections followed the stated Spark, end-to-end, live and PostgreSQL marker filter.

**Environment** Local macOS host; isolated Python 3.12 test environment, portable Java 21, bundled Node.js and locally downloaded checksum-verified release tooling. Build dependency resolution used an isolated environment. External credentials or live cloud deployments were not introduced.

**Snapshot discipline** The broad test run started before all follow-up edits were complete. Targeted reruns verify the changed behavior. A final immutable checkout must still run all GitHub gates before tagging; no single local result here is represented as complete cross-platform certification.

# Relationship to the original review

The supplied response reports fixes for all 18 original findings. That is useful evidence, but its Linux verification is not the same as acceptance on the target systems. The table separates that prior remediation from the additional work performed here. See C01 to C14 for the actual follow-up changes.

| **ID** | **Original issue**                    | **Follow up status**                                                                              |
|--------|---------------------------------------|---------------------------------------------------------------------------------------------------|
| F01    | Job credential escalation             | Prior fix inspected; retain real adversarial worker verification.                                 |
| F02    | SQL host access                       | Further tightened here with a finite function allowlist and fail-closed parsing, C01.             |
| F03    | Read-only Kubernetes mounts           | Prior remediation; real Helm/schema checks add evidence, real kubelet still required.             |
| F04    | Livy submission replay                | Residual 503 replay behavior corrected here, C02.                                                 |
| F05    | Helm network ports                    | Real Helm and kubeconform checks passed; enforcing CNI remains to be checked.                        |
| F06    | Windows-only import failure           | Prior portability remediation; real Windows workflow still required.                              |
| F07    | Remote results not delivered          | Prior provider fix; verify real pod termination/result delivery.                                  |
| F08    | Lost or duplicated logs               | Prior cursor/log fix; real kubelet bursts and reconnects remain to verify.                        |
| F09    | Parameter defaults                    | Prior generated-source fix; live Livy parameters remain to verify.                                |
| F10    | SQL script splitting                  | Prior generated-source fix; real runtime dialect corpus remains to verify.                        |
| F11    | Batch argument contract               | Prior request coverage; real Livy batch acceptance required.                                 |
| F12    | Reserved Spark configuration          | Prior provider fix; inspect effective configuration in remote Spark.                              |
| F13    | Timeout overlaps and duplicate writes | Additional unsafe-503 and macOS cancellation fixes, C03 and C04.                                  |
| F14    | Remember-login client behavior        | Prior client fix; Node reporter harness corrected here, C14. Multi-tab and browser checks remain. |
| F15    | Test module collisions                | Current test collection succeeds; run actual GitHub collection/order gates.                       |
| F16    | Required mypy gate                    | Required CLI/SDK command passes locally.                                                          |
| F17    | macOS environment assumptions         | Local tests and release build add evidence; clean-host installer acceptance remains.              |
| F18    | Unseeded pip in CI                    | Prior seeding fix; release build/install validation succeeds locally.                             |

The original review’s 7 high-priority findings should not be treated as production-closed solely because a unit test passed. Their remaining live acceptance conditions are captured in R03 through R10. New or residual high-priority changes in this follow-up are C01 through C05.

# Applied changes

Each entry below describes changes already present in the latest folder. References point to the edited files. The short code examples express the relevant policy and are simplified illustrations where indicated; use the source file for the exact implementation.

## C01 Reject unapproved Spark SQL functions and unparsed writer statements

**Priority** P1

**Problem** The remediation relied on a denylist of known host-access functions. An unknown registered UDF, a qualified function, or a future built-in could bypass that model. A writer also had a fallback which accepted text the SQL parser could not fully classify. Those paths weakened the intended boundary between SQL data access and arbitrary code execution.

**Applied change** Added an explicit, finite allowlist of approved Spark function names and rejected unknown or qualified functions. Removed the permissive unparsed-text fallback for writers. All roles now need a single fully parsed, supported statement. Regression cases cover unknown functions, qualified UDFs, function creation, and multiple statements.

**Behavior and limitations** This deliberately tightens compatibility: custom functions and unsupported dialect constructs now fail closed. Do not restore a text pass-through to make a query work. Review and test each function before adding it to the allowlist. The guard remains one layer of isolation; it does not make notebooks safe for untrusted tenants.

**Verification** The focused SQL guard and SQL session tests passed in the 265-pass targeted run. Live execution through a real pooled Spark kernel remains a release prerequisite.

Policy illustration

if fn not in SPARK_ALLOWED_FUNCTIONS:  
raise Unsupported(...)  
\# Qualified functions are also rejected.  
\# Parse failures now raise Unsupported for every role.

`src/olcf/sql/guard.py line 29`

`src/olcf/sql/spark_functions.py line 1`

`tests/data/test_sql_host_guard.py line 435`

## C02 Stop replaying uncertain Livy submissions after HTTP 503

**Priority** P1

**Problem** The provider treated a 503 response to a submission as proof that no work was created and retried the POST. A gateway can return 503 after the upstream service accepted a job. Repeating that submission can run a notebook or write operation twice.

**Applied change** Removed 503 from the safe submission retry condition. A POST with an ambiguous server error now raises SubmissionUncertain so reconciliation can determine what happened. Known rejection handling for 429 is retained; safe reads and deletes retain their retry behavior. Updated regression cases to classify both 500 and 503 as uncertain.

**Behavior and limitations** A transient 503 can now require reconciliation instead of immediate replay. This trades an apparent fast recovery for protection against duplicate side effects. The same rule must remain consistent at every outer retry layer.

**Verification** The focused Livy regression cases passed. Mock transport evidence does not establish how every Livy proxy behaves. Exercise an actual server where the response is lost after acceptance.

Policy illustration

POST + HTTP 503 -\> SubmissionUncertain  
GET or DELETE + HTTP 503 -\> existing safe retry policy

`src/olcf/compute/providers/livy.py line 19`

`tests/compute/test_review_livy.py line 212`

## C03 Apply the same uncertainty rule to pipeline HTTP activities

**Priority** P1

**Problem** Pipeline HTTP activities also classified 503 as definitely not processed. A side-effecting request without an idempotency key could be sent again after the remote service had committed it.

**Applied change** Removed 503 from NOT_PROCESSED_STATUSES. An unsafe request that may have been sent is treated as uncertain and does not repeat automatically. Safe methods and explicitly idempotent requests retain their supported retry behavior. The 503 regression now expects one attempt and one POST.

**Behavior and limitations** A failed activity may need operator reconciliation even when the remote service eventually completed its action. A caller-supplied idempotency key is useful only if the destination actually enforces that key. Do not assume every endpoint does so.

**Verification** All 57 tests in the final timeout, runtime-trust and release-artifact targeted run passed. Additional socket cancellation regressions passed separately.

Policy illustration

NOT_PROCESSED_STATUSES = {408, 425, 429}  
\# A 503 is no longer evidence that an unsafe request had no effect.

`src/olcf/pipelines/activities/http.py line 50`

`tests/orch/test_timeout_overlap.py line 277`

## C04 Let the connection owner close an aborted socket

**Priority** P1

**Problem** The local macOS tests intermittently left an HTTP worker blocked in recv after the cancellation watcher called shutdown and close from another thread. Four targeted cases failed; a repeated single-case probe passed four times and failed on the fifth. A stack trace showed the read still blocked during cancellation.

**Applied change** After a successful socket shutdown, the cancellation path now returns without closing the stream from the watcher thread. The I/O owner performs normal cleanup. A connection that completes after cancellation is still explicitly closed by its owning connection thread before it can send data. Added real socket-pair tests for wake-up, descriptor ownership and cancellation-before-send.

**Behavior and limitations** This addresses the observed shutdown/close race without declaring a timed-out task finished while it can still write. It does not undo a remote commit and does not change the need to classify uncertain results. Streams without a usable socket still use the existing close fallback.

**Verification** Eight consecutive stress repetitions passed after the change, at about 2.3 seconds each. The two new socket ownership tests and the final timeout regression file passed. This is meaningful local evidence, not a universal guarantee for every TLS implementation or operating system.

Policy illustration

sock.shutdown(socket.SHUT_RDWR)  
return \# the I/O owner closes the stream  
  
\# A connection created after abort is closed before hand-off.

`src/olcf/pipelines/ssrf.py line 196`

`tests/orch/test_connection_tracker.py line 1`

## C05 Make unsafe production runtime trust fail readiness

**Priority** P1

**Problem** The runtime-trust check could report DOWN when production trusted the public development signing key, while the readiness route still returned success. A load balancer or orchestrator could consider that configuration ready.

**Applied change** Registered runtime trust as a critical check and made readiness return HTTP 503 when its status is DOWN. Liveness remains available. Added explicit production and development cases, updated the existing CLI integration test from its former 200-readiness contract to 503, and regenerated the health-check reference to match the implementation.

**Behavior and limitations** Production deployments that explicitly trust the public development key now fail readiness. Missing operator anchors and development-mode behavior follow their existing status rules. This does not automatically create production keys or re-sign the shipped manifests.

**Verification** All 30 runtime-trust tests passed; they were also included in the final 57-test targeted run. The tests cover production failure, allowed development use, and liveness without leaking sensitive details.

Policy illustration

if ctx.health.run("runtime-trust").status is Status.DOWN:  
return JSONResponse(..., status_code=503)  
  
\# runtime-trust is registered as critical.

`src/olcf/api/core/admin.py line 191`

`src/olcf/security/runtime_trust.py line 66`

`tests/security/test_runtime_trust.py line 288`

`tests/tooling/test_runtime_trust_cli.py line 169`

`docs/reference/health-checks.md line 35`

`SECURITY.md line 7`

## C06 Repair the source archive gate and build both distributions

**Priority** P2

**Problem** The existing build produced a source archive containing public tests, CI files and deploy/.env.example, then rejected its own archive because the wheel exclusion rules were applied to source packages too. The release script also omitted the separate Fabric SDK distribution and deleted a pre-existing output directory.

**Applied change** Separated source-archive allowances from installed-wheel restrictions. Only source archives may include tests, CI files and the exact public deploy/.env.example file; private keys, databases, real environment files and bytecode remain forbidden. The build now creates the Fabric SDK wheel and source archive, checks SDK/server version agreement, validates SDK wheel metadata and required modules, and includes Apache LICENSE and NOTICE. Nonempty output directories are refused instead of deleted.

**Behavior and limitations** The release now includes two independently installable distributions. A source archive is intentionally more complete than a wheel. The SDK wheel has a dedicated gate; extending equivalent metadata/member validation to the SDK source archive is still recommended.

**Verification** The full release build completed with a clean wheel installation, olcf init, SBOM, license notices and verified SHA256SUMS. Regression tests cover allowed source files, forbidden secret/cache files and license metadata fallback.

Policy illustration

server wheel + server sdist + Fabric SDK wheel + Fabric SDK sdist  
existing nonempty output directory -\> error, preserved contents

`scripts/verify_dist.py line 11`

`scripts/build_release.sh line 64`

`sdk/fabric/LICENSE line 1`

`sdk/fabric/NOTICE line 1`

`tests/tooling/test_release_artifacts.py line 1`

## C07 Use valid license classifiers when legacy metadata is unknown

**Priority** P2

**Problem** The notice gate failed on ptyprocess because its legacy License field was UNKNOWN even though its metadata contained a valid ISC classifier. The parser treated the placeholder as authoritative and missed the available license identification.

**Applied change** Treat UNKNOWN, NONE and NOASSERTION as missing metadata and continue to supported classifiers. Added a regression test using the UNKNOWN-plus-ISC-classifier shape. The upstream ptyprocess 0.7.0 license was checked to confirm the ISC identification.

**Behavior and limitations** This does not waive unidentified-license failures or replace a licensing review. It fixes metadata precedence so a placeholder does not conceal useful evidence. The generated release notices remain an artifact of the resolved dependency environment.

**Verification** The license regression and release notice generation passed. Official reference: https://raw.githubusercontent.com/pexpect/ptyprocess/0.7.0/LICENSE

Policy illustration

License: UNKNOWN  
Classifier: License :: OSI Approved :: ISC License (ISCL)  
resolved license -\> ISC

`scripts/licenses.py line 70`

`tests/tooling/test_release_artifacts.py line 1`

## C08 Require validation before the release build

**Priority** P2

**Problem** The release path was not structurally dependent on all of the existing validation workflows. Cross-workflow concurrency groups could also cancel another run using the same ref. The pip-audit command combined --disable-pip with a resolved requirements file without declaring the no-dependency-resolution mode it required.

**Applied change** Made CI, deployment and security workflows reusable with workflow_call. The release build now needs those three workflows to succeed. Concurrency groups distinguish the calling workflow. Invocations explicitly use Bash for the release script. Added --no-deps to pip-audit for the already resolved requirements list.

**Behavior and limitations** A tag build can take substantially longer because it waits for the broad checks. Actual GitHub permissions, runner behavior, action availability and nested-workflow behavior still need a repository dry run. Syntax checking alone cannot validate them.

**Verification** Actionlint passed on all workflow files. The corrected local resolved-runtime dependency audit reported no known vulnerabilities. The workflows have not been executed on GitHub in this review.

Policy illustration

build:  
needs: \[verify, containers, security\]  
  
pip-audit ... --disable-pip --no-deps

`.github/workflows/ci.yml line 13`

`.github/workflows/container.yml line 15`

`.github/workflows/security.yml line 14`

`.github/workflows/release.yml line 9`

## C09 Separate artifact preparation from package publication

**Priority** P2

**Problem** The original flow could publish immediately, used an invalid comma-separated attestation subject pattern, and treated a non-hyphenated 0.x tag as a stable latest release. The installer command also depended on a package registry name rather than the exact generated wheel.

**Applied change** GitHub releases are now drafts; all 0.x versions are prereleases and do not move the latest container tag. Disabled metadata-action’s automatic latest flavor so only the explicit stable-release rule can create that tag. Signing and image jobs require a tag. Attestation subjects use separate newline patterns. Image-signing owner names are lowercased. Release notes install the exact tag wheel and describe beta limitations. Optional PyPI publication moved to a separate release-published workflow which downloads the reviewed packages, verifies their checksums, and uses trusted publishing only when PUBLISH_PYPI=true.

**Behavior and limitations** The maintainer must review and publish the draft. Important: a tag still pushes versioned container images before the draft is published; the draft gate is not a complete external-publication gate. Configure environment approval before images as well if all publication must require a final human action.

**Verification** Workflow syntax passed. No release, image, signature, attestation or PyPI upload was performed. Test both server and Fabric SDK trusted-publisher configuration before enabling package publication.

Policy illustration

draft: true  
prerelease: startsWith(tag, "v0.") or contains(tag, "-")  
PyPI trigger: release published, with PUBLISH_PYPI=true

`.github/workflows/release.yml line 9`

`.github/workflows/pypi.yml line 1`

## C10 Add a release identity preflight and private contact

**Priority** P2

**Problem** Public-facing metadata still contains placeholder GitHub owner/repository values and an invalid CODEOWNERS entry. The conduct contact was unset, and the security contact needed an actionable private route.

**Applied change** Added a non-mutating preflight over the README, contributor/security/conduct documents, citation, CODEOWNERS and issue-template configuration. Release builds stop if identity placeholders remain. Set jdsql@hotmail.com as the conduct contact and a private vulnerability-reporting address, and retained the instruction to enable GitHub private reporting. Corrected the security document to describe pre-submission credential registration and the new readiness behavior.

**Behavior and limitations** The email is configured, but the actual GitHub owner and repository still need to be supplied and applied consistently. No remote repository was created or changed. The preflight is intentionally failing on the unresolved identity files.

**Verification** The preflight reported README.md, CONTRIBUTING.md, CITATION.cff, .github/CODEOWNERS and .github/ISSUE_TEMPLATE/config.yml. It no longer reports the conduct or security contact.

Policy illustration

python scripts/check_release_config.py  
\# Current result: release identity metadata is incomplete.

`scripts/check_release_config.py line 1`

`.github/workflows/release.yml line 9`

`CODE_OF_CONDUCT.md line 41`

`SECURITY.md line 7`

## C11 Make secret scanning useful without hiding test coverage

**Priority** P2

**Problem** The local scanner reported intentionally synthetic credentials, generated bytecode, a public development key, and copied password output in documentation. Leaving those unresolved would make the security gate noisy or failing; broadly excluding tests would hide future real leaks.

**Applied change** Added narrowly scoped scanner allowances for generated bytecode and the exact intentionally public development key. Marked individual synthetic fixture lines with gitleaks:allow comments; test files remain scanned. Replaced copied example password output with a placeholder and simplified an idempotency-key example that resembled a secret. Added ignore rules for local keys, environment files, macOS metadata, test artifacts and Hypothesis cache, retaining the public development-key exception.

**Behavior and limitations** The public development key remains public by design and must never become a production trust anchor. A clean directory scan does not examine absent Git history or prove no credentials exist. Newly added ignore rules do not remove secrets that are already committed elsewhere.

**Verification** Gitleaks completed with no remaining findings after the narrow corrections. The appendix lists every fixture file changed; those edits are scanner comments and do not change the intended test logic.

Policy illustration

Default gitleaks rules remain enabled.  
No blanket exclusion of tests or application source was added.

`.gitignore line 18`

`.gitleaks.toml line 1`

`docs/getting-started/README.md line 114`

`docs/guides/cli.md line 137`

`examples/rest_curl.md line 56`

## C12 Align release documentation with current evidence

**Priority** P2

**Problem** The existing README described the whole platform as implemented despite known missing capabilities, and release instructions did not reflect the new gates, SDK artifacts, draft release or separate PyPI workflow. Generated health documentation became stale after the runtime-trust change.

**Applied change** Narrowed the implementation claim to the core platform; removed the fixed test-count claim; recorded actual Helm/kubeconform and macOS follow-up evidence without implying full platform support. Added a release-readiness checklist. Updated the release process for reusable gates, Bash invocation, source-versus-wheel rules, SDK version checks, nonempty output protection, drafts and PyPI publication. Regenerated reference documentation; only the health-check reference changed in substance.

**Behavior and limitations** Historical review responses remain historical evidence. The comprehensive report is the authoritative record of this follow-up, including recommendations which were not implemented. Documentation must be updated again after live acceptance tests.

**Verification** Documentation links, CLI examples, settings, states and generated-page checks passed after regeneration. Header checks passed on 618 Python files.

Policy illustration

Beta for evaluation on trusted infrastructure.  
Fabric, CNI, Livy, Windows and recovery acceptance still required.

`README.md line 261`

`docs/development/release-process.md line 5`

`docs/development/release-readiness.md line 1`

`docs/reference/health-checks.md line 35`

## C13 Scope the credential regression to its own workspace

**Priority** P2

**Problem** The broad run failed an assertion that exactly two provider submissions had been intercepted. The same case passed alone and failed when its whole test file ran. The file shares a module-level control plane, so earlier tests can leave queued work which the global provider monkeypatch also observes.

**Applied change** Restricted the injected WorkerOffline failure and captured callback tokens to the newly created workspace for this test. Other work delegates directly to the real provider. The test still requires exactly two submissions, distinct tokens, rejection of the abandoned token and acceptance of the replacement token.

**Behavior and limitations** This changes test isolation, not production credential permissions or scheduler behavior. It avoids mistaking another test’s background job for an extra attempt of the target job. Token values are not reproduced in this report.

**Verification** The isolated case passed before the test correction, while the full file reproduced the problem. Repeated full-file results after correction are listed in the validation record.

Policy illustration

if req.job\["workspace_id"\] != ws:  
return real(self, req)  
\# Inject and assert only for this test workspace.

`tests/compute/test_job_credentials.py line 304`

## C14 Select a stable reporter for the browser client test wrapper

**Priority** P2

**Problem** All six Node browser-client scenarios passed, but their Python wrapper failed because it searched for the TAP summary text “# pass 6”. The installed Node version used a different default reporter, producing a human-readable summary instead.

**Applied change** Passed --test-reporter=tap explicitly when the Python harness invokes node --test. Kept both the successful-exit assertion and the six-passed-scenarios assertion, so an empty or silently skipped run does not count as success.

**Behavior and limitations** This is a test portability correction. It does not modify the browser authentication client or weaken any of the six remember-login scenarios. Real-browser and multi-tab acceptance still remain separate checks.

**Verification** The broad run’s captured output showed six passes and zero failures in Node itself. The corrected Python wrapper is included in the final affected-file rerun recorded in the validation matrix.

Policy illustration

node --test --test-reporter=tap remember_auth.test.mjs

`tests/web/test_remember_auth.py line 36`

# Recommendations still to implement or verify

These items are intentionally separate from the applied-change register. A recommendation is not evidence that the implementation exists or the live test has passed. Timing describes the claim or deployment it should block, so optional features need not all become prerequisites for publishing a narrowly scoped beta source repository.

## R01 Complete repository identity and repository controls

**Timing** Before first public release

Replace owner and repository placeholders in the files reported by scripts/check_release_config.py, select real CODEOWNERS reviewers, and check package URLs, badges, issue links, citation dates and installation commands. Enable private vulnerability reporting, branch protection and required status checks. Confirm that jdsql@hotmail.com is monitored for both conduct and security reports.

**Acceptance test** the identity preflight passes; every public link resolves to the intended repository; a fork pull request runs read-only checks; a maintainer can receive a private report.

## R02 Rehearse the entire GitHub release workflow

**Timing** Before first public release

Run workflow_dispatch with dry_run=true on the actual repository. Resolve runner, permissions, registry, action and reusable-workflow failures. Review the draft and its install command before publication. The tag workflow currently publishes versioned GHCR images before the draft is approved; add a protected environment to the image job if that is too early. Configure both Python package names for trusted publishing, because the build now produces olcf and olcf-fabric. GitHub’s prerelease flag does not make PyPI version 0.1.0 a prerelease. Before publishing a PyPI beta, align PEP 440 package versions, tag filters, version comparisons and installer asset names; rehearse that exact version convention.

**Acceptance test** all required jobs pass on GitHub, a clean machine verifies checksums and provenance, the wheel installs, the SDK imports, and a draft is reviewed before release publication. No credentials should be required in untrusted fork jobs.

## R03 Establish a private signing and runtime distribution process

**Timing** Before enabling production runtimes

Generate operator-owned signing keys outside the repository, store them with restricted access, configure trust anchors, and re-sign runtime manifests. Do not ship a private production key. Test archive downloads, checksums, dependency installation, offline bundles and rollback on each supported architecture. Keep development trust explicitly limited to local development.

**Acceptance test** production installs accept the intended signed runtime and reject modified archives, unknown signatures and the public development key; readiness fails for the unsafe trust configuration.

## R04 Provide real worker and Spark isolation

**Timing** Before advertising secure multi-tenant compute

Notebook and job code executes with the worker OS user’s privileges. A SQL guard does not create an operating-system sandbox. Isolate tenant or trust groups using separate worker identities/containers and restrictive filesystems, network access and secret mounts. Spark RPC authentication and encryption are not configured by OLCF, so private networking remains essential. Define which users are trusted to run arbitrary code.

**Acceptance test** adversarial jobs cannot read another tenant’s files or credentials, impersonate another worker, reach administrative services, or attach to another tenant’s Spark process. Until that is demonstrated, describe the release as trusted-user beta compute.

## R05 Exercise a cluster with an enforcing network policy

**Timing** Before enabling Kubernetes in supported production

Run the Helm chart against a Kubernetes API and kubelet with non-root containers and an enforcing CNI. Validate Secret mounts, writable work/output paths, driver/executor connectivity, callback delivery, cancellation, cleanup, lost-pod recovery and result extraction. Stress log bursts, truncation and reconnect cursors; the supplied response notes quadratic behavior under dense bursts.

**Acceptance test** a two-pod Spark application completes behind the actual NetworkPolicy; prohibited egress is blocked; results and logs survive reconnects; duplicate or orphaned jobs do not appear after cancellation or controller restart.

## R06 Verify ambiguity and recovery against a Livy gateway

**Timing** Before enabling Livy in supported production

Submit notebook and batch jobs to Livy through the intended gateway. Lose the connection after acceptance and generate a 503 after upstream commit. Confirm one logical execution, correct reconciliation, parameter delivery, SQL script order, result handling and required Spark configuration. Test 429 separately; retain its special handling only when the deployment’s proxy contract establishes rejection.

**Acceptance test** uncertain submissions do not blindly resubmit; a late remote success is discoverable; failed and cancelled work revokes the right callback credentials; no duplicate external writes occur.

## R07 Validate Fabric Entra and OneLake in a tenant

**Timing** Before claiming Microsoft integration support

Use a test tenant to validate capsule acquisition, notebook export/import, token audiences, permissions, tenant scoping, workspace boundaries, OneLake read/write and remote status/error mapping. Exercise expired tokens, revoked consent, throttling and interrupted submissions. Keep the documented assumptions visible until real service behavior confirms them.

**Acceptance test** an authorized Fabric notebook executes remotely with intended data access; cross-tenant and underprivileged calls are refused; callbacks and result artifacts remain scoped to the initiating workspace.

## R08 Run installation and lifecycle acceptance on clean hosts

**Timing** Before claiming full Windows or macOS support

The current macOS run adds useful evidence but does not replace a clean-machine installer exercise. Run Windows CI on Windows hosts, test PowerShell installation, service start/stop, upgrade/uninstall and the msvcrt lock. On macOS test without Command Line Tools and with realistic filesystem permissions. Test Linux service installation with systemd and supported distribution paths.

**Acceptance test** fresh install, first-run login, notebook execution, stop/start, upgrade and rollback pass on each advertised OS; no missing pip, fcntl import, shell or compiler assumptions remain.

## R09 Prove recovery of jobs sessions and kernels

**Timing** Before relying on recovery guarantees

The supplied remediation notes describe active-job reconciliation but leave session/kernel recovery and clean-host restoration insufficiently demonstrated. Restore a backup onto a clean host with the required keys. Reconcile stale session and kernel records against live workers; explicitly terminate or reattach orphaned resources rather than leaving them apparently active.

**Acceptance test** restored data is usable, secrets decrypt, active-job state is reconciled, stale sessions cannot issue callbacks, and the runbook explains recoverable versus unrecoverable in-flight work.

## R10 Exercise PostgreSQL worker TLS and rotation under failure

**Timing** Before multi-node production

Run separate control-plane nodes and workers with shared PostgreSQL, real mTLS, certificate renewal and key rotation. Partition nodes during logout, token revocation and job completion. Choose an explicit worker-token lifetime instead of relying on the nonexpiring default. Define per-node rate-limit behavior behind the production load balancer.

**Acceptance test** revocation converges within the documented window, rotation does not strand work or weaken trust, stale certificates fail, and concurrent controllers do not duplicate execution.

## R11 Broaden supply chain and package validation

**Timing** Before a stable release

Pin third-party GitHub Actions by reviewed commit SHA and maintain updates. Add an explicit SDK source-archive gate, plus a clean SDK install/import smoke test. Audit vendored JavaScript and runtime/JDK manifests in addition to the resolved Python environment. Review actual Linux and Windows dependency resolutions. Generate release notices from the shipped environment and review unresolved license obligations.

**Acceptance test** every release artifact has required metadata and license files, contains no unintended secrets/cache files, installs independently where appropriate, and can be verified by checksum/signature. Advisory scans cover the actual release platforms and have a documented triage process.

## R12 Expand the allowlist only with runtime evidence

**Timing** Before broad SQL compatibility claims

The new allowlist closes unknown-function execution paths but can reject legitimate Spark syntax, including parser-normalized function forms not yet covered. Test a representative query corpus against each supported Spark version. Ensure pooled sessions cannot register or shadow an approved function name with hostile code. Keep notebook execution and SQL-only execution in separate trust contexts where necessary.

**Acceptance test** approved queries work on the real runtime; unknown, qualified, reflective and host-access functions fail; session reuse does not create a bypass. Every allowlist addition has a focused positive and negative test.

## R13 Keep scope and roadmap explicit

**Timing** Before publishing feature promises

Retain the known gaps: general JDBC/ODBC gateway and database connectors, hierarchical external catalogs, Iceberg management beyond discovery, R/Scala/C# notebook cells, signed OS installers, per-pool isolation and Spark RPC protection. Distinguish Java/Scala jar jobs from notebook-cell support. Avoid describing these items as working features merely because configuration or interface scaffolding exists.

**Acceptance test** README, release notes, requirements traceability and examples agree on implemented, experimental and planned capabilities. Each planned capability has an issue with its own acceptance tests.

## R14 Verify documentation against the published assets

**Timing** Before directing users to installation one liners

Replace unresolved download URLs and use exact release wheel URLs until PyPI publication is established. Confirm virtual-environment activation or use uv run consistently in source-install examples. Test commands as a new user, including runtime-signing prerequisites and failure recovery. Keep generated references synchronized after behavioral changes.

**Acceptance test** a reader can follow the published quickstart from a clean host without inventing a URL, activating an undocumented environment or trusting a public production signing key.

# Release sequence

**1.** Review the applied changes and unresolved broad-test results. Preserve the user’s original ZIP and the latest edited source separately.

**2.** Choose and apply the real GitHub owner/repository. Run the release metadata preflight until it passes. Confirm the private reporting contact and repository controls.

**3.** Publish source as a scoped beta only after all relevant local checks are green. Keep Kubernetes, Livy, Fabric and Windows claims marked experimental until acceptance is recorded.

**4.** Run the complete GitHub dry run from an immutable commit, including required Python, Spark, browser, Windows, PostgreSQL, deployment and security jobs. Triage every failure and every unexpected skip.

**5.** Complete the live acceptance exercises for each feature that will be advertised as supported. Record the service versions, topology and observed results.

**6.** Provision private runtime signing, review license notices and advisory findings, and build fresh artifacts from the final immutable source. Verify both Python distributions independently.

**7.** Tag the approved version, inspect all artifacts and signatures, and check the draft release installation instructions on clean machines. Remember that the tag pipeline can already publish versioned GHCR images.

**8.** Publish the reviewed draft only when ready. Enable PyPI trusted publishing only after both projects and the protected environment are configured. Keep a rollback and advisory-response procedure available.

## Suggested validation commands

Run from the repository with its documented development environment active. These commands do not install infrastructure or publish anything. A complete GitHub run additionally covers the platform-specific jobs configured in CI.

ruff check src tests sdk scripts examples deploy/scripts

mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent

python scripts/check_headers.py  
python scripts/check_docs.py  
python scripts/check_release_config.py

python -m pytest -m "not spark and not e2e and not live and not postgres" -q

python -m pytest tests/orch/test_connection_tracker.py tests/orch/test_timeout_overlap.py -q

actionlint -shellcheck= -pyflakes= .github/workflows/\*.yml  
gitleaks dir . --redact

bash scripts/build_release.sh --out /path/to/new-empty-release-directory

# Complete changed file inventory

This inventory compares file contents with the saved baseline made before the follow-up edits. Added and removed line counts summarize text differences; they are not a count of defects or proof of coverage. Line references elsewhere identify the first changed area, not every changed line. Generated runtime caches and local test artifacts are excluded.

| **File**                                | **Status** | **Lines plus minus** | **Change**                               |
|-----------------------------------------|------------|----------------------|------------------------------------------|
| .github/workflows/ci.yml                | Modified   | +2 / -1              | C08                                      |
| .github/workflows/container.yml         | Modified   | +2 / -1              | C08                                      |
| .github/workflows/pypi.yml              | Added      | +47 / -0             | C09                                      |
| .github/workflows/release.yml           | Modified   | +37 / -24            | C08, C09, C10                            |
| .github/workflows/security.yml          | Modified   | +5 / -4              | C08                                      |
| .gitignore                              | Modified   | +12 / -0             | C11                                      |
| .gitleaks.toml                          | Added      | +10 / -0             | C11                                      |
| CODE_OF_CONDUCT.md                      | Modified   | +1 / -1              | C10                                      |
| README.md                               | Modified   | +6 / -8              | C12                                      |
| SECURITY.md                             | Modified   | +3 / -7              | C05, C10                                 |
| docs/development/release-process.md     | Modified   | +14 / -14            | C12                                      |
| docs/development/release-readiness.md   | Added      | +16 / -0             | C12                                      |
| docs/getting-started/README.md          | Modified   | +1 / -1              | C11                                      |
| docs/guides/cli.md                      | Modified   | +1 / -1              | C11                                      |
| docs/reference/health-checks.md         | Modified   | +1 / -1              | C05, C12                                 |
| examples/rest_curl.md                   | Modified   | +1 / -1              | C11                                      |
| scripts/build_release.sh                | Modified   | +7 / -1              | C06                                      |
| scripts/check_release_config.py         | Added      | +32 / -0             | C10                                      |
| scripts/licenses.py                     | Modified   | +3 / -1              | C07                                      |
| scripts/verify_dist.py                  | Modified   | +33 / -9             | C06                                      |
| sdk/fabric/LICENSE                      | Added      | +202 / -0            | C06                                      |
| sdk/fabric/NOTICE                       | Added      | +23 / -0             | C06                                      |
| src/olcf/api/core/admin.py              | Modified   | +4 / -2              | C05                                      |
| src/olcf/compute/providers/livy.py      | Modified   | +5 / -5              | C02                                      |
| src/olcf/pipelines/activities/http.py   | Modified   | +2 / -2              | C03                                      |
| src/olcf/pipelines/ssrf.py              | Modified   | +4 / -0              | C04                                      |
| src/olcf/security/runtime_trust.py      | Modified   | +1 / -1              | C05                                      |
| src/olcf/sql/guard.py                   | Modified   | +8 / -36             | C01                                      |
| src/olcf/sql/spark_functions.py         | Added      | +56 / -0             | C01                                      |
| tests/compute/test_job_credentials.py   | Modified   | +4 / -0              | C13                                      |
| tests/compute/test_review_livy.py       | Modified   | +2 / -2              | C02                                      |
| tests/data/test_sql_host_guard.py       | Modified   | +10 / -1             | C01                                      |
| tests/e2e/test_12_security_admin.py     | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/fabric/test_capsule.py            | Modified   | +2 / -2              | C11 synthetic fixture scanner annotation |
| tests/fabric/test_e2e_security.py       | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/items/test_git_sync.py            | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/items/test_sparkjobs_variables.py | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/ops/test_diagnostics.py           | Modified   | +4 / -4              | C11 synthetic fixture scanner annotation |
| tests/orch/test_activities.py           | Modified   | +3 / -3              | C11 synthetic fixture scanner annotation |
| tests/orch/test_alerts.py               | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/orch/test_connection_tracker.py   | Added      | +56 / -0             | C04                                      |
| tests/orch/test_timeout_overlap.py      | Modified   | +3 / -1              | C03                                      |
| tests/security/test_authorization.py    | Modified   | +2 / -2              | C11 synthetic fixture scanner annotation |
| tests/security/test_leaks.py            | Modified   | +2 / -2              | C11 synthetic fixture scanner annotation |
| tests/security/test_runtime_trust.py    | Modified   | +15 / -0             | C05                                      |
| tests/security/test_streams.py          | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/tooling/test_deploy.py            | Modified   | +1 / -1              | C11 synthetic fixture scanner annotation |
| tests/tooling/test_release_artifacts.py | Added      | +33 / -0             | C06, C07                                 |
| tests/tooling/test_runtime_trust_cli.py | Modified   | +3 / -3              | C05                                      |
| tests/web/test_remember_auth.py         | Modified   | +1 / -1              | C14                                      |

# Source and evidence references

Primary local source: the latest extracted project folder named on the opening page. The user-supplied CODE_REVIEW_RESPONSE.md and the existing requirements traceability and contracts documents provide historical assertions; independent results are labeled in the validation record. The prior compiled review dated 8 October is superseded only where this follow-up explicitly describes a later change or result.

`User supplied remediation response`

`Previous compiled code review`

`Requirements traceability`

External primary references support the retry policy, workflow structure and specific license-metadata check. They do not certify this project’s implementation.

[HTTP Semantics RFC 9110 on idempotency and retries](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)

[GitHub documentation on reusable workflows](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows)

[Docker metadata action v5 tag and flavor rules](https://github.com/docker/metadata-action/tree/v5#latest-tag)

[Upstream ptyprocess 0.7.0 ISC license](https://raw.githubusercontent.com/pexpect/ptyprocess/0.7.0/LICENSE)

**Final interpretation** The fixes and tests substantially improve release readiness. The remaining work is specific and reviewable, but local success must not be translated into an unsupported claim that every feature works in production.
