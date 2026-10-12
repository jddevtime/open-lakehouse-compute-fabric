Prepared for the project owner • 8 October 2026

# Review conclusion

The project has confirmed defects and is not ready for a full production release. The review identifies 18 actionable findings, including 7 high-priority issues involving authorization, SQL isolation, duplicate execution, Kubernetes deployment and Windows startup. Several remaining capabilities also lack the live-system evidence required by the master requirements.

The core has substantial implementation and automated-test coverage. Both Python distributions build, and the repository-wide Ruff check passes. Those strengths do not resolve the runtime and contract defects described here. Prioritize the P1 findings before exposing the service to untrusted users or relying on remote writes.

**Scope** The review covers the extracted OLCF 0.1.0 source tree, its Fabric SDK, UI, deployment files, tests and the supplied master requirements. Source inspection concentrated on authorization boundaries, execution state transitions, provider contracts, data paths and operational recovery. Automated checks cover a broader portion of the repository. This is a broad risk-based review, not proof that every line or every possible deployment has been verified.

**Change status** No fixes were applied to the original project. Tests ran in an isolated copy; this document is the compiled issue list and implementation guidance requested.

**Evidence labels** Reproduced means observed with the real local code, sometimes with a clearly described test transport. Source-confirmed means the producer/consumer or control-flow mismatch was inspected. Live verification gaps are listed separately and are not asserted to be proven runtime failures.

**Priority** P1 means fix before production or before using the affected feature. P2 means a correctness, result-delivery or required validation issue that should be addressed before release. Priorities are engineering recommendations, not a formal CVSS score.

# Finding index

| **ID** | **Priority** | **Problem**                                                                                                                                                 |
|--------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| F01    | P1           | [Job credentials expand the submitting token permissions](#f01-job-credentials-expand-the-submitting-token-permissions)                                     |
| F02    | P1           | [Read only Spark SQL accepts host capability expressions](#f02-read-only-spark-sql-accepts-host-capability-expressions)                                     |
| F03    | P1           | [The default Kubernetes runner writes into a read only volume](#f03-the-default-kubernetes-runner-writes-into-a-read-only-volume)                           |
| F04    | P1           | [Livy retries writes after ambiguous transport failures](#f04-livy-retries-writes-after-ambiguous-transport-failures)                                       |
| F05    | P1           | [The default Helm policy blocks Spark master traffic](#f05-the-default-helm-policy-blocks-spark-master-traffic)                                             |
| F06    | P1           | [Unix only imports prevent Windows application startup](#f06-unix-only-imports-prevent-windows-application-startup)                                         |
| F07    | P2           | [The Kubernetes completion result is never transmitted by the stock runner](#f07-the-kubernetes-completion-result-is-never-transmitted-by-the-stock-runner) |
| F08    | P2           | [Remote log paging can discard the final result and later log lines](#f08-remote-log-paging-can-discard-the-final-result-and-later-log-lines)               |
| F09    | P2           | [Livy notebook defaults overwrite submitted parameters](#f09-livy-notebook-defaults-overwrite-submitted-parameters)                                         |
| F10    | P2           | [Livy splits SQL inside quoted string literals](#f10-livy-splits-sql-inside-quoted-string-literals)                                                         |
| F11    | P2           | [Livy batch submission reads arguments from the wrong fields](#f11-livy-batch-submission-reads-arguments-from-the-wrong-fields)                             |
| F12    | P2           | [Livy silently drops submitted Spark configuration](#f12-livy-silently-drops-submitted-spark-configuration)                                                 |
| F13    | P1           | [A timed out pipeline activity can overlap its retry](#f13-a-timed-out-pipeline-activity-can-overlap-its-retry)                                             |
| F14    | P2           | [Remembered sign in is cleared after a page reload and token refresh](#f14-remembered-sign-in-is-cleared-after-a-page-reload-and-token-refresh)             |
| F15    | P2           | [Test helper imports depend on collection order](#f15-test-helper-imports-depend-on-collection-order)                                                       |
| F16    | P2           | [The required CLI and SDK type check fails](#f16-the-required-cli-and-sdk-type-check-fails)                                                                 |
| F17    | P2           | [Platform assumptions break the macOS test gate](#f17-platform-assumptions-break-the-macos-test-gate)                                                       |
| F18    | P2           | [CI does not install pip even though publishing and SDK tests invoke it](#f18-ci-does-not-install-pip-even-though-publishing-and-sdk-tests-invoke-it)       |

Select a problem title in the index to jump to its detailed finding. Detailed findings follow the validation results. Each includes source locations, the problematic logic, consequences, evidence, a suggested repair and a regression check. Section numbers at the end refer to the supplied master requirements. Code excerpts are shortened for clarity; the linked source and line ranges are authoritative.

# Source snapshot

/Users/aimac/Desktop/\_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender

The inventory contains 967 non-cache files, including 568 Python files across source, tests, SDKs, tooling and examples. The application source contains 312 Python files and the tests contain 227. Third-party vendored browser libraries were inventoried but were not independently audited line by line. No comprehensive dependency vulnerability or license audit was performed.

Master requirements: [Open Lakehouse Fabric Compute Extender Master Requirements](file:///Users/aimac/Downloads/Open_Lakehouse_Fabric_Compute_Extender_Master_Requirements.docx)

# Validation results

The corrected broad suite completed, but it did not pass cleanly. The remaining failures were triaged as test-harness/platform/path-assertion issues; they still block the required test gate. Independent targeted reproductions also demonstrate the product defects in the finding list.

| **Check**                    | **Observed result**                                                                                                                                                                                                                                        |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Isolated development install | Succeeded with olcf\[dev\] on macOS 26 arm64 and Python 3.12.14.                                                                                                                                                                                           |
| Broad automated suite        | 5 failed, 2503 passed, 40 skipped, 105 deselected, 10 warnings, 11 errors in 1835.33s (0:30:35)                                                                                                                                                            |
| Ruff                         | Passed for src tests sdk scripts examples deploy/scripts.                                                                                                                                                                                                  |
| Python compilation           | compileall passed for src sdk scripts.                                                                                                                                                                                                                     |
| JavaScript syntax            | Node syntax checks passed for all 49 first-party UI JavaScript files.                                                                                                                                                                                      |
| CLI and SDK type check       | Failed: 2 errors in 2 files; 29 source files checked. See F16.                                                                                                                                                                                             |
| Package builds               | olcf and olcf-fabric wheels both built successfully.                                                                                                                                                                                                       |
| Documentation consistency    | Links, CLI commands, settings and state diagrams passed; generated docs/reference/cli.md was stale.                                                                                                                                                        |
| Local Spark SQL probe        | Spark 4.1.1 with Java 21 executed the harmless reflection and script-transform expressions accepted by the Viewer planner. This was separate from the full OLCF pooled-kernel path.                                                                        |
| Targeted reproductions       | Confirmed scope expansion, SQL planner acceptance, Livy replay, notebook parameter loss, SQL splitting, batch argument loss, runner result mismatch, bounded log-window loss, timeout overlap, remembered-login loss and simulated Windows import failure. |

**Command** pytest -m "not spark and not e2e and not live and not postgres" -v -p no:cacheprovider --timeout=300 --junitxml=../pytest-final.xml

**Environment correction** The first broad run was stopped after 10 minutes 43 seconds because missing Java, missing pip and an unconfigured system Git stub contaminated the results. A portable Java 21 runtime, working Git and pip were provided in the isolated review environment, then the entire selected suite was rerun. The initial partial run is not the headline result.

**Not exercised** The repository Spark/Delta test suite, PostgreSQL, browser end-to-end tests, Docker/Helm installation, actual Windows, cross-machine Spark and production HA/failover were not executed. A separate temporary-prefix macOS installer run is described below. The excluded markers are listed in the command; some unmarked integration tests use local test doubles and real Python workers.

The run selected 2,558 of 2,663 collected tests; 105 were deselected by the marker expression. Fixture teardown errors may be reported in addition to a test-body result, so the pytest result counts should not be summed as distinct selected tests.

A separate canonical-path run of all ten real-agent integration tests passed in 34.16 seconds. The workspace roundtrip test passed alone in 3.48 seconds; the host-Python script test passed with a canonical interpreter path in 6.67 seconds. These targeted results diagnose the broad-suite failures; they do not convert the broad run into a pass.

Forty broad-suite tests were skipped: 6 needed the cached OLCF Spark runtime; 25 installer cases could not see uv on that run's PATH; 9 needed deployment tools (kubeconform, Helm, Docker or systemd-analyze). A supplemental installer run exposed the available uv and completed with 20 passed, 2 failed and 3 skipped in 56.42 seconds. Its skips were two unavailable shellcheck checks and an x86_64-only download-stub fixture on arm64. These supplemental counts are reported separately, not added to the broad-suite total.

The initial unseeded uv environment also exposed a reproducibility defect: olcf\[dev\] does not guarantee pip, while publishing/build code invokes python -m pip. See F18. Installing pip corrected that prerequisite for the final broad run.

## Failure triage

**Collection order imports** 11 failure/error reports. Ten real-agent setup errors and the workspace roundtrip assertion fail because bare conftest imports resolve to a different directory in the full run. See F15. The full real-agent file passes all ten tests when run separately; the roundtrip test also passes alone. Move helpers to qualified support modules and require the complete suite to pass, not only isolated files.

**Host platform assumptions** 4 failure/error reports. Two environment tests and the hostsafe test assume Linux; the first-run integration fixture scans /proc on macOS during teardown. See F17. The native-wheel and source-distribution failures reproduce alone. Use actual target platform tags and portable child-process cleanup. The teardown error is distinct from an error in the test body.

**Interpreter path assertion** 1 failure/error reports. tests/node/test_host_python.py line 57 compares sys.executable as a literal string. The broad run invoked ../review-venv/bin/python, while the worker printed the equivalent canonical absolute path. The job had already succeeded. Repeating the relative invocation reproduced the assertion failure; running through a canonical absolute interpreter path passed. Normalize both paths with Path.resolve or os.path.samefile before comparison. This is a test robustness issue, not an observed worker execution failure.

## Dependency versions

fastapi 0.143.0; starlette 1.7.0; pydantic 2.13.5; sqlalchemy 2.1.4; httpx 0.28.1; sqlglot 30.21.0; duckdb 1.5.6; deltalake 1.6.6; pyarrow 25.0.1; pytest 9.1.1; mypy 2.4.0; ruff 0.16.10; pip 26.2.1; websockets 17.2.

The project specifies broad lower dependency bounds. These results apply to the versions above; they do not establish compatibility with every allowed version. Freeze an approved release/development environment and rerun CI before shipping.

# Detailed findings

## F01 Job credentials expand the submitting token permissions

**Priority** P1

[src/olcf/compute/jobs.py lines 851 to 856](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

[src/olcf/security/rbac.py lines 119 to 127](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/security/rbac.py)

pid = row\["submitted_by"\] or SYSTEM_PRINCIPAL.id  
... scopes=\["job"\], ... meta={"job_id": row\["id"\], ...}

**Failure and impact** A Contributor using a restricted mcp:write credential can submit a job whose derived credential has secret.use and lakehouse.write, even though those permissions were excluded from the submitting token. The derived token retains the user identity but replaces the token permission ceiling. Job metadata alone does not preserve that ceiling. This defeats the purpose of a restricted automation credential and can expose operations the caller intentionally could not perform.

**Evidence** Reproduced through the actual identity, workspace, job-submission and authentication services in an isolated database. The submitted Python job was queued. Before minting, secret.use=False and lakehouse.write=False; authenticating the derived job token changed both to True. The reproduction called the dispatch token-minting method directly; it did not execute an adversarial notebook.

**Suggested fix** Persist the effective permission ceiling and workspace constraints with each submission. Derive an attempt credential from the intersection of that ceiling, current principal bindings and the minimum job permissions. Enforce job and workspace binding in authentication, including secret resolution. Audit session and pipeline credential derivation for the same pattern. Revoke derived credentials when the parent or attempt is revoked.

**Regression check** Submit with a restricted PAT that can create jobs but cannot use secrets or write data. Run a job that attempts both operations and require denial. Add cross-workspace, revoked-parent, session and nested-pipeline cases. An unrestricted Contributor should retain only the documented job capabilities.

Master requirements sections 22, 23, 31.

## F02 Read only Spark SQL accepts host capability expressions

**Priority** P1

[src/olcf/sql/guard.py lines 185 to 228](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/sql/guard.py)

[src/olcf/sql/service.py lines 285 to 317](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/sql/service.py)

[src/olcf/compute/sessions.py lines 732 to 760](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/sessions.py)

if isinstance(ast, exp.Query):  
\_check_nodes(ast)  
return Statement(ast, "select", True, dialect)

**Failure and impact** The SQL guard treats a SELECT query as read-only after a short denylist check. Spark REFLECT, JAVA_METHOD and script TRANSFORM expressions are not rejected. They provide capabilities beyond reading authorized lakehouse tables. A Viewer can therefore reach host inspection or script execution primitives through an interface intended to require only sql.execute. The exact executable capabilities depend on the deployed Spark version and sandbox.

**Evidence** The real OLCF SQL planner accepted REFLECT, JAVA_METHOD and SELECT TRANSFORM('hello') USING 'cat' AS (value) for a Viewer without job.submit, marking all three read_only=True. In a separate isolated Spark 4.1.1 local runtime with Java 21, both reflection variants returned the harmless java.version property and the script transform returned "hello" through cat. This confirms actual engine capability as well as planner acceptance. The two stages were tested separately; an entire authenticated OLCF HTTP request through its pooled production kernel was not exercised.

**Suggested fix** Apply a Spark-specific AST and function allowlist for the read-only SQL surface. Reject script transformations, reflection and any function with filesystem, network, process or session capabilities before allocating a kernel. Keep execution sandboxed with a narrowly scoped service identity. Do not assume SELECT syntax establishes safety.

**Regression check** Require these queries to fail in planning for Viewer and read-only token roles, before session creation. Repeat the harmless reflection and cat-transform checks through the authenticated OLCF SQL endpoint on the supported Spark runtime. Preserve ordinary scalar, aggregate, CTE and lakehouse SELECT behavior.

Master requirements sections 18, 22, 31.

## F03 The default Kubernetes runner writes into a read only volume

**Priority** P1

[src/olcf/compute/providers/kubernetes.py lines 339 to 353](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/kubernetes.py)

[src/olcf/runtime_lib/runner.py lines 127 to 139](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/runtime_lib/runner.py)

[src/olcf/runtime_lib/runner.py lines 417 to 423](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/runtime_lib/runner.py)

command = \[..., "/opt/olcf/job/job.json"\]  
"volumeMounts": \[{"mountPath": "/opt/olcf/job", "readOnly": True}\]  
\# runner  
workdir = job_path.parent  
\_write_json_atomic(workdir / "result.json", doc)

**Failure and impact** The stock provider places job.json and source in a Kubernetes Secret mounted at the same path that the runner uses as its writable work directory. Secret volumes are read-only. Notebooks need to create kernel files there, and every completion writes result.json there. Consequently, the default runner cannot complete its normal contract even when user code itself needs no output files. A custom runner_command may work around this, but the default is broken.

**Evidence** Generated the actual pod manifest and inspected its command, mounts and volumes. It contains only the read-only Secret for the job directory and no writable staging volume. Traced the runner workdir calculation and result writer. Kubernetes documentation independently confirms that Secret volumes are always read-only. A live Kubernetes cluster was not available.

**Suggested fix** Mount the Secret at a separate input path. Copy validated input files to an emptyDir using an init container or bootstrap step, then run from that writable directory. Set ownership and permissions for the chosen non-root UID. Keep credentials readable only by the job and remove them after execution. Use a persistent or uploaded channel for artifacts that must survive pod cleanup.

**Regression check** Run the stock image and generated manifest on a real cluster with a non-root container. Verify Python script success, notebook execution, failure result, cancellation, output notebook and artifact retrieval. Confirm the Secret remains read-only while the work directory is writable.

Master requirements sections 13, 14, 26, 31.

## F04 Livy retries writes after ambiguous transport failures

**Priority** P1

[src/olcf/compute/providers/livy.py lines 63 to 86](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

[src/olcf/compute/providers/livy.py lines 88 to 113](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

for attempt in range(3):  
r = self.\_http.request(method, ...)  
\# every HTTPError retries, regardless of method  
\# 429 502 503 504 also retry

**Failure and impact** The same retry loop is used for GET and for POST operations that create sessions, statements or batches. A server may accept a statement and execute a write before the response is lost. Retrying the POST then creates another execution. The OLCF job idempotency key does not make Livy statement creation idempotent. This can silently duplicate data changes even when the job is not declared idempotent.

**Evidence** A MockTransport simulated a lost response on the first statement POST and success on the second. LivyClient issued two POST requests and returned the second statement id. The reproduction verifies client behavior under ambiguous delivery, not a real external duplicate write. Livy REST documentation identifies these POST endpoints as creation operations.

**Suggested fix** Make retries operation-specific. Retry reads and failures proven to occur before transmission. Persist submission intent and reconcile ambiguous creates against remote state before another attempt. Where Livy cannot establish whether a statement was accepted, return an explicit uncertain outcome instead of replaying a non-idempotent write. Carry this policy across session, statement and batch creation.

**Regression check** Use a test server that records and commits a statement, then closes the connection before responding. Assert one remote execution and an explicit recoverable/uncertain state. Also test throttling before acceptance, safe GET retries and controller restart during statement submission.

Master requirements sections 13, 33.

## F05 The default Helm policy blocks Spark master traffic

**Priority** P1

[deploy/helm/olcf/templates/networkpolicy.yaml lines 59 to 66](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/deploy/helm/olcf/templates/networkpolicy.yaml)

[deploy/helm/olcf/values.yaml lines 96 to 105](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/deploy/helm/olcf/values.yaml)

[src/olcf/core/config.py lines 93 to 94](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/core/config.py)

\# worker ingress permits only  
- {port: 41000, endPort: 41199, protocol: TCP}  
\# configured master port bases  
spark_master_port_base = 7077  
spark_master_ui_port_base = 18080

**Failure and impact** NetworkPolicy and workers are enabled by default. On a cluster whose CNI enforces NetworkPolicy, the worker ingress rule allows driver/block-manager ports but excludes the Spark master RPC and UI ports. A master hosted on a worker pod cannot be reached normally by sibling workers and health/status callers. This undermines the default two-worker deployment. Disabling all network isolation is not an appropriate permanent fix.

**Evidence** Compared the rendered-template port rules with the configured master port bases and standalone provider allocation. The mismatch is source-confirmed. The control-plane restrictive egress branch has a related omission when egressAllowAll is set to false.

**Suggested fix** Define explicit configurable ranges for master RPC, master UI, worker RPC, driver and block manager ports, and derive daemon allocation and policy rules from the same settings. Limit permitted peers by labels. Check dynamically allocated worker daemon ports as well as the two obvious master ports. Include required monitoring paths in restrictive egress rules.

**Regression check** Install the chart using its defaults on a NetworkPolicy-enforcing cluster. Start a Spark application across two worker pods and prove both executors participate. Repeat with restrictive egress enabled; verify monitoring, cancellation and rescheduling while unrelated ingress remains blocked.

Master requirements sections 13, 26, 32.

## F06 Unix only imports prevent Windows application startup

**Priority** P1

[src/olcf/notebooks/gitsync.py lines 27 to 30](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/notebooks/gitsync.py)

[src/olcf/notebooks/layer.py lines 21 to 24](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/notebooks/layer.py)

[src/olcf/notebooks/module.py lines 36 to 44](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/notebooks/module.py)

[tests/conftest.py line 8](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/conftest.py)

import fcntl  
\# module install imports GitService and EnvironmentService  
\# layer creation also calls fcntl.flock directly

**Failure and impact** The notebook Git and environment-layer modules import fcntl unconditionally. Python does not provide this module on Windows. The normal application module installation imports these components, so the issue can stop application startup before a user invokes Git or environment publishing. The shared test conftest has the same import and prevents a Windows suite from collecting.

**Evidence** A local import-hook simulation made fcntl unavailable and then called create_app with a fresh test configuration. Startup raised ModuleNotFoundError: No module named fcntl. This is a focused simulation of the missing module, not execution on Windows. The repository already has a cross-platform lock implementation in runtime/locks.py that demonstrates the intended abstraction.

**Suggested fix** Replace direct Unix locking with a tested cross-platform file-lock abstraction. Keep the layer helper compatible with its standard-library-only runtime contract. Ensure lock acquisition and release work with Windows byte-range locking semantics, including creating a lockable byte. Remove unconditional fcntl imports from test collection and add Windows to the normal CI matrix.

**Regression check** On an actual supported Windows host, create the full app, start an agent, publish an environment, contend for a layer lock and perform a Git status/commit cycle. Run test collection before any platform-specific skip logic. Verify a crashed process does not leave an unusable lock.

Master requirements sections 1, 12, 21, 26, 35.

## F07 The Kubernetes completion result is never transmitted by the stock runner

**Priority** P2

[src/olcf/runtime_lib/runner.py lines 127 to 139](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/runtime_lib/runner.py)

[src/olcf/compute/jobs.py lines 1480 to 1516](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

\_write_json_atomic(workdir / "result.json", doc)  
emit_event("runner_finished", state=state, category=...)  
\# controller expects event == "result" with a result document

**Failure and impact** Even after fixing the writable-volume issue, the default runner writes its exit value, metrics, artifacts and error details only to a local result.json file. The remote polling path consumes a result event from logs, but the runner emits runner_finished without that payload. A successful pod is then finalized with only {state: succeeded}. User-visible results can be missing despite reported success.

**Evidence** Called the real runner finish method in a temporary directory while capturing emit_event calls. The result file contained expected-value, but the sole event was runner_finished with state and category. Inspected the remote monitor, which reads a result event and otherwise falls back to a minimal success document. This is a contract mismatch; no live pod was needed to observe it.

**Suggested fix** Define one explicit remote completion contract. Prefer an authenticated result/artifact upload endpoint or durable object reference bound to job and attempt. If a result event remains supported, have a Kubernetes wrapper emit the documented payload exactly once after successful persistence. Treat missing required completion data as an incomplete result that must be reconciled.

**Regression check** Execute a notebook returning a structured exit value and output artifact through the stock Kubernetes provider. Assert the API exposes both. Test failure traceback, metrics, cancellation, pod deletion and controller restart. Require the contract to work independently of ordinary log volume.

Master requirements sections 9, 14, 24, 33.

## F08 Remote log paging can discard the final result and later log lines

**Priority** P2

[src/olcf/compute/jobs.py lines 1481 to 1516](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

[src/olcf/compute/providers/kubernetes.py lines 429 to 441](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/kubernetes.py)

[src/olcf/compute/providers/kubernetes.py lines 139 to 141](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/kubernetes.py)

page = provider.logs(handle, offset, 262144)  
...  
if st.state == "succeeded":  
self.\_finish(...) \# page.eof is not checked

**Failure and impact** The monitor reads only one page before finalizing a terminal remote application. If a job finishes between polls with more than 256 KiB of unread output, the remaining pages and any result event in them are not drained by subsequent active-job polls. Separately, Kubernetes log retrieval is capped at 8 MiB but its cursor is an absolute line count into each retrieved snapshot; this cannot reliably resume a changing or truncated snapshot.

**Evidence** The terminal-page omission is directly visible in the monitor. A bounded-window provider reproduction returned line1/line2, then line2/line3; continuing at offset 2 returned an empty page and lost line3. This models a truncated or rotated log window, not a live Kubernetes log server. Both problems require a durable cursor rather than an assumption that every snapshot begins at the original first line.

**Suggested fix** Separate terminal process status from completion of log/result ingestion. Drain until the provider indicates a stable end before closing ingestion, with bounded work and persisted offsets across polls. Use timestamps plus disambiguation, sequence identifiers or a dedicated log store to handle truncation and rotation. Report explicit gaps when the remote source can no longer supply earlier bytes.

**Regression check** Finish a job before its first poll after writing more than 256 KiB and placing a result at the end; require the result and full retained log to arrive. Repeat above 8 MiB, with log rotation and with a controller restart. Assert no silent loss and no duplicate ingestion.

Master requirements sections 14, 28, 34.

## F09 Livy notebook defaults overwrite submitted parameters

**Priority** P2

[src/olcf/compute/providers/livy.py lines 133 to 164](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

[src/olcf/notebooks/params.py lines 381 to 398](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/notebooks/params.py)

header = "# --- OLCF parameters ---\n" + assignments  
...  
return header + "\n\n".join(parts)

**Failure and impact** Livy flattens a notebook by prepending submitted parameter assignments before every notebook cell. A normal tagged parameters cell then executes its defaults and overwrites those assignments. The same notebook therefore runs with different values depending on its compute provider, potentially reading or writing the wrong partition, region or date.

**Evidence** Reproduced with a tagged parameters cell setting region="default" and a submitted value region="requested". Executing the generated source left the observed value as "default". The ordinary notebook path already has a shared parameter injection implementation; the Livy path bypasses its placement semantics.

**Suggested fix** Normalize the notebook and use the shared parameter injection logic before flattening. Place injected assignments after the designated defaults cell, or replace defaults according to the existing notebook contract. Preserve tags, type validation and cell order. Fail clearly on unsupported formats rather than silently changing parameter semantics.

**Regression check** Run the same parameterized notebook through agent and Livy providers and compare results. Cover tagged defaults, no parameters cell, multiline source arrays, falsy values, structured values and unsafe parameter names. Existing values must not be overwritten by defaults.

Master requirements sections 8, 9, 14, 19.

## F10 Livy splits SQL inside quoted string literals

**Priority** P2

[src/olcf/compute/providers/livy.py lines 140 to 142](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

[tests/compute/test_providers_livy.py lines 65 to 70](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/compute/test_providers_livy.py)

stmts = \[s.strip() for s in text.split(";") if s.strip()\]

**Failure and impact** A valid SQL script containing a semicolon inside a string literal or comment is split into invalid fragments. For example SELECT 'a;b' AS value becomes two spark.sql calls with unterminated strings. This breaks legitimate SQL workloads. The current test explicitly expects the naive split, so the test preserves the defect instead of detecting it.

**Evidence** Reproduced the generated Python for SELECT 'a;b' AS value;. It calls spark.sql("SELECT 'a") and spark.sql("b' AS value"). The repository test calls this behavior documented. This report therefore treats it as an existing documented limitation that still needs a code fix for correct SQL support, not a newly discovered hidden behavior.

**Suggested fix** Use a Spark-dialect parser or a lexical statement splitter that handles quoted literals, escaped quotes and comments. Reuse the same splitting semantics as other SQL execution paths. If only one statement is supported, reject multiple statements explicitly while preserving a semicolon inside a literal.

**Regression check** Replace the test that expects three statements from select 1; select 'a;b' with an assertion for two intact statements. Add escaped quotes, line/block comments, trailing delimiters, Unicode values and malformed input diagnostics.

Master requirements sections 14, 18.

## F11 Livy batch submission reads arguments from the wrong fields

**Priority** P2

[src/olcf/compute/providers/livy.py lines 203 to 208](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

[src/olcf/compute/jobs.py lines 893 to 895](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

"args": spec\["parameters"\].get("args", \[\])  
if spec\["parameters"\].get("main_class"):  
body\["className"\] = spec\["parameters"\]\["main_class"\]

**Failure and impact** The normalized job spec stores a Spark job definition main_class and args in source, but Livy batch submission reads similarly named entries from parameters. A normal JAR or Python Spark job therefore loses its configured arguments; a JAR may lose the className required to select its entry point. Adding unrelated parameters can accidentally alter execution instead.

**Evidence** Called the actual ExternalProvider Livy batch path with source.artifact_uri, source.main_class="example.Main" and source.args=\["one", "two"\], using a local test client. The captured request had args=\[\] and omitted className. This confirms the normalized-source metadata is lost without contacting a remote service.

**Suggested fix** Read source.args and source.main_class from the normalized job contract. Validate their types once and apply consistent behavior across local Spark submit and Livy batch paths. Keep notebook/business parameters distinct from executable argument metadata. Reject a missing required main class with an actionable error.

**Regression check** Create a Spark job definition with a JAR URI, a main class and two arguments. Capture the outgoing Livy request and assert file, className and args match exactly. Repeat for a Python artifact and compare with the agent-backed provider.

Master requirements sections 8, 13, 14.

## F12 Livy silently drops submitted Spark configuration

**Priority** P2

[src/olcf/compute/jobs.py lines 811 to 819](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

[src/olcf/compute/jobs.py lines 893 to 895](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/jobs.py)

[src/olcf/compute/providers/livy.py lines 171 to 184](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/compute/providers/livy.py)

spark_block = None  
if engine == "spark" and provider.prepares_spark:  
spark_block = provider.prepare(... spark_conf=...)  
\# Livy has prepares_spark = False but reads spec\["spark"\]\["conf"\]

**Failure and impact** Dispatch builds a Spark configuration block only when prepares_spark is true. Livy disables that preparation, and \_build_spec receives None. Livy then reads its session or batch configuration from this absent block. Consequently, accepted spark_conf values can be silently ignored, changing job behavior or preventing required Spark settings from taking effect.

**Evidence** The producer/consumer path is source-confirmed: user spark_conf is supplied only to provider.prepare, that call is skipped for Livy, and both Livy session and batch bodies obtain conf from spec.spark.

**Suggested fix** Separate provider-neutral Spark configuration from agent-specific cluster preparation. Always carry validated user configuration into the job spec, then let each provider filter prohibited driver/network settings and translate supported values. Reject unsupported settings during submission rather than silently discarding them. Treat credentials separately from public configuration.

**Regression check** Submit a Livy job with a harmless setting such as spark.sql.shuffle.partitions and assert the outgoing conf contains it. Verify the value inside a real remote Spark session. Cover batch and notebook paths and confirm forbidden driver/network overrides remain rejected.

Master requirements sections 12, 13, 14, 21.

## F13 A timed out pipeline activity can overlap its retry

**Priority** P1

[src/olcf/pipelines/engine.py lines 1070 to 1079](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/pipelines/engine.py)

[src/olcf/pipelines/engine.py lines 1090 to 1111](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/pipelines/engine.py)

[src/olcf/pipelines/activities/http.py lines 197 to 213](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/pipelines/activities/http.py)

err = ActivityError(..., category="timeout", retryable=True)  
self.\_conclude(task, error=err)  
task.cancel.set()  
\# async cancellation is then best effort in another task

**Failure and impact** A timeout first concludes the attempt and can schedule its retry, then signals cancellation. The previous thread or remote activity may still be executing, and HTTP POST timeouts are independently marked retryable. With retries enabled, a second attempt may begin while the original request is still able to commit. The policy.idempotent field is documented for crash recovery, so configuring retries is not evidence that an ambiguous external write is safe to repeat.

**Evidence** Reproduced with the real in-process pipeline engine and a local test activity that takes two seconds and does not cooperate with cancellation. With timeoutSeconds=1, retries=1, retryBackoffSeconds=0 and idempotent=false, attempt 2 started approximately 1.05 seconds after attempt 1 started, while attempt 1 finished at approximately 2.00 seconds. The attempts overlapped for about 0.95 seconds. No external write was performed. The source ordering also explains why delayed HTTP/job cancellation can have the same consequence.

**Suggested fix** Introduce an uncertain/cancelling phase for timed-out side effects. Do not release an activity for replay until the original is confirmed stopped or its operation has a stable externally enforced idempotency key. Distinguish pre-send failures from accepted-but-unanswered requests. Explicitly define retry safety for HTTP, SQL, notebook and job activities and surface an operator decision when the outcome is unknown.

**Regression check** Use an HTTP endpoint that commits after the pipeline timeout while delaying its response. Set retries=1 and verify the effect occurs once. Repeat with a non-cooperative activity and a job whose cancel request is delayed. Assert no concurrent attempts unless their operation contract explicitly makes overlap safe.

Master requirements sections 19, 33.

## F14 Remembered sign in is cleared after a page reload and token refresh

**Priority** P2

[src/olcf/web/static/js/api/client.js lines 21 to 41](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/web/static/js/api/client.js)

const blob = { access: auth.access, refresh: auth.refresh, exp: auth.exp };  
if (s && (s.access \|\| s.refresh)) { Object.assign(auth, s); return true; }  
// auth.remember stays false; persist then deletes prefs auth

**Failure and impact** The session-storage blob does not include remember. After a reload, loadStoredAuth chooses the session blob before checking the persistent preference. The fresh in-memory remember flag remains false. The next token refresh invokes persist and deletes the local-storage refresh token, so Keep me signed in stops working after the normal reload-and-refresh sequence.

**Evidence** Executed the actual client module logic in Node with isolated storage and timer mocks. Signed in with remember=true, recreated the page context, loaded stored auth and refreshed tokens. Persistent auth existed before reload and was absent after refresh. This covers the state logic, not a browser end-to-end flow.

**Suggested fix** Persist and restore the remember preference explicitly, or reconcile the session token with the persistent preference before returning from loadStoredAuth. Retain intentional logout clearing and avoid making a session-only sign-in persistent. Consider multi-tab refresh rotation separately.

**Regression check** Add a browser test for remembered sign-in, reload, refresh, closing/reopening the browser and logout. Add the same flow with remember=false and require no persistent credentials. The existing Node reproduction can serve as a fast unit regression.

Master requirements sections 22, 25.

## F15 Test helper imports depend on collection order

**Priority** P2

[tests/compute/test_real_agent.py lines 89 to 92](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/compute/test_real_agent.py)

[tests/items/test_roundtrip_samples.py lines 91 to 92](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/items/test_roundtrip_samples.py)

@pytest.fixture(scope="module")  
def make_stack_module(...):  
from conftest import Stack

**Failure and impact** The full test run loads several files named conftest.py. A runtime import of the bare module name can resolve to another test directory rather than the intended helper. The real-agent fixture fails before its tests execute, even though running its file alone resolves the intended class. This makes the documented whole-suite CI command unreliable and hides important integration coverage.

**Evidence** The initial broad run produced ten real-agent setup errors: cannot import name Stack from conftest, resolving to tests/security/conftest.py. Running tests/compute/test_real_agent.py alone got through setup and reached the job assertion; that separate run initially failed because Java was absent. The two failures have different causes. The corrected broad run is summarized in the validation section.

**Suggested fix** Move reusable helpers out of conftest.py into uniquely named importable test-support modules and import them by qualified package path. Keep conftest for fixture registration. Audit deferred import conftest calls, including the layer tests that access conftest.present. Do not suppress these setup errors as expected platform skips. The workspace roundtrip test also dynamically imports conftest.present and WIDE_MODULES; move those into the same kind of explicit support module.

**Regression check** Run the full documented test command and targeted files in different collection orders. Assert that real-agent tests execute in each run. Add a lightweight collection/import smoke check and ensure macOS/Windows-specific helpers use portable process discovery instead of unconditional /proc access.

Master requirements sections 35.

## F16 The required CLI and SDK type check fails

**Priority** P2

[src/olcf/cli/commands/auth.py lines 280 to 282](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/cli/commands/auth.py)

[src/olcf/cli/commands/lifecycle.py lines 642 to 647](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/cli/commands/lifecycle.py)

[.github/workflows/ci.yml lines 83 to 87](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/.github/workflows/ci.yml)

roles=bindings \# list\[dict\] passed to list\[Mapping\]  
...  
if getattr(exc, "remediation", None):  
o.info(f" hint: {exc.remediation}")

**Failure and impact** The CI workflow treats the CLI/SDK mypy check as a hard gate, but the same command fails with two errors. One is an invariant collection annotation mismatch; the other accesses an attribute not declared on Exception. These are build-quality failures, not evidence that ordinary user creation or quickstart currently crashes at runtime.

**Evidence** Ran mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent in the isolated development environment. It reported two errors in two files across 29 checked source files. The ignore comment on the continuation line did not suppress the error at auth.py line 281.

**Suggested fix** Have the SDK accept a read-only Sequence\[Mapping\[str, Any\]\] if it only reads bindings, or type the constructed list consistently. Capture remediation = getattr(exc, "remediation", None) and use that variable after checking it, or narrow to the appropriate exception type. Pin a reviewed development dependency set so CI behavior is reproducible.

**Regression check** Rerun the exact hard-gate mypy command and require zero errors. Keep user creation and quickstart failure-path tests passing. Avoid broad ignores that hide unrelated type mistakes.

Master requirements sections 24, 35.

## F17 Platform assumptions break the macOS test gate

**Priority** P2

[tests/items/test_environments.py lines 273 to 330](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/items/test_environments.py)

[tests/e2e/firstrun.py line 78](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/e2e/firstrun.py)

[tests/e2e/stack.py line 175](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/e2e/stack.py)

[.github/workflows/ci.yml lines 99 to 110](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/.github/workflows/ci.yml)

[tests/tooling/test_hostsafe.py lines 181 to 192](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/tooling/test_hostsafe.py)

[tests/tooling/test_installer.py lines 230 to 240](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/tooling/test_installer.py)

[tests/tooling/test_installer.py lines 277 to 301](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/tests/tooling/test_installer.py)

assert host.startswith("linux")  
...  
for d in Path("/proc").iterdir():

**Failure and impact** The CI matrix includes macOS, but environment tests assert that the current host is Linux and first-run teardown scans /proc unconditionally. These are test portability defects that fail a supported CI platform rather than proving the application behavior is broken. Teardown failures can additionally leave uncertainty about child-process cleanup.

**Evidence** On macOS arm64, the native-wheel test fails because host is macos-arm64 rather than Linux. The source-distribution test targets a platform that differs from the resolving host and is correctly rejected by the application. The first-run integration fixture reports FileNotFoundError for /proc during teardown. The two environment failures also reproduce when their tests run alone. The broad run also failed test_protect_environment_only_on_macos at tests/tooling/test_hostsafe.py line 186 because its first assertion assumes Linux before mocking is_macos. Explicitly mock the Linux branch before testing the macOS branch. A supplemental installer run reported 20 passed, 2 failed and 3 skipped. The two failures read .bashrc after installation selected the zsh profile for this Mac. Installation and earlier assertions succeeded; the final purge step in the uninstall test was not reached because the profile assertion failed.

**Suggested fix** Construct target wheel tags from the actual host and explicitly parameterize foreign-platform cases. Keep the build-from-source acceptance case on the resolving platform. Replace /proc iteration with psutil or another portable process-tree API, scoped to processes started by the fixture. Reserve platform skips for cases whose behavior is actually platform-specific. Set the intended shell in each fixture and parameterize Bash/zsh profile expectations instead of always reading .bashrc.

**Regression check** Run the selected suite and first-run cleanup on Linux and macOS, and add Windows where supported. Verify both positive native-wheel/sdist behavior and negative cross-platform rejection. Assert fixture-created children are gone at teardown without scanning an unavailable filesystem.

Master requirements sections 26, 35.

## F18 CI does not install pip even though publishing and SDK tests invoke it

**Priority** P2

[.github/workflows/ci.yml lines 104 to 108](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/.github/workflows/ci.yml)

[src/olcf/notebooks/piplock.py lines 176 to 183](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/src/olcf/notebooks/piplock.py)

[pyproject.toml lines 72 to 76](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/pyproject.toml)

[scripts/build_fabric_sdk.sh lines 28 to 29](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/scripts/build_fabric_sdk.sh)

\# CI creates a uv environment and installs the dev extra  
uv venv --python ...  
uv pip install -e ".\[dev\]"  
\# Environment publishing invokes  
\[python or sys.executable, "-m", "pip", \*args\]

**Failure and impact** The documented CI environment creation does not seed pip, and the dev dependency set does not require it. Environment publishing and the Fabric SDK build script invoke python -m pip directly. A clean environment can therefore install the project successfully with uv but fail these features/tests with No module named pip. This is a reproducibility/prerequisite defect rather than a package-resolution problem in the submitted environment.

**Evidence** The review created an isolated environment with uv and installed olcf\[dev\]. python -m pip reported No module named pip. The initial suite then failed environment publication and SDK packaging. Installing pip in that isolated environment allowed those paths to run in the corrected suite; the remaining environment failures were the separate platform assumptions in F17. The CI workflow uses the same unseeded creation pattern.

**Suggested fix** Seed the CI environment explicitly or include pip in the supported development/runtime tooling dependencies where required. Add an actionable environment-publishing prerequisite check. Decide whether pip is a guaranteed control-plane dependency or an isolated resolver tool, and keep installer, uv and ordinary venv setup instructions consistent.

**Regression check** Start with an empty uv environment, execute the exact CI setup commands, then publish an offline test environment and build the Fabric SDK. Require success without an undocumented manual pip installation. Test the missing-tool error path as well.

Master requirements sections 21, 24, 35.

# Existing limitations requiring release decisions

The repository already acknowledges the following limitations. They remain material to the requested full-platform acceptance criteria. They are not counted again as new findings. The status descriptions below summarize the repository traceability document and the inspected implementation; they are not a new independent certification of every requirement.

[docs/compliance/requirements-traceability.md](file:///Users/aimac/Desktop/_projects/Open_Lakehouse_Fabric_Compute_Extender_zipextracted/Open_Lakehouse_Fabric_Compute_Extender/docs/compliance/requirements-traceability.md)

## Production runtime trust

Runtime signing defaults to a bundled development public key whose matching private key is shipped. runtime/signing.py lines 96–112 allow that key unless explicitly disabled or replaced. Require an operator-owned trust store for production and fail startup/readiness when development trust remains enabled. Prove that a development-signed manifest is rejected in production.

## Execution isolation and transport

Notebook code runs as the worker OS account. The sandbox command is an optional hook, and host-Python shares the worker environment. Spark RPC authentication/encryption are not configured by OLCF. Use per-pool OS/container isolation, restricted filesystem/network access and authenticated/encrypted Spark transport where supported; test lateral access and credential exposure before multi-tenant deployment.

## Credential lifecycle

Master-key and JWT-key rotation lack supported administrative entry points. Worker mTLS certificates expire after 90 days without renewal or expiry alerts; worker tokens do not expire. Implement rotation, staged trust overlap, renewal and rollback with multi-controller propagation and explicit expiry diagnostics.

## Fabric and storage evidence

Run supported source/context acquisition, default lakehouse, permission denial, cancellation, result and secret-redaction cases against the target tenant and runtime. Verify MinIO/S3 and ADLS using real service endpoints as well.

## Data format interoperability

The repository notes that its real Spark/Delta interoperability gate had not run. API-level protocol checks do not alone prove that arbitrary Spark notebook writes remain within policy. Run Spark, delta-rs and the supported external engine against the same tables, checking data and protocol before and after writes. Establish how policy applies to direct worker writes and reject unsupported features before mutation.

## Feature scope gaps

The repository documents no JDBC/ODBC gateway, no general database connection types, no catalog schema/view/function hierarchy or Hive/REST/Glue/Unity adapters, and incomplete Iceberg management. Scala/R/C# cells are not executed. Either implement the required scope or explicitly narrow the release acceptance criteria and product claims.

## Packaging and first run

There are no signed MSI, deb or rpm packages in the documented scope. The supplemental macOS installer run exercised temporary-prefix installation, startup, upgrade, rollback and parts of uninstall; its profile assertions still need repair. Docker, Helm, Windows and system-service activation still need real acceptance runs. Host-Python fallback is chosen once at submission; jobs queued before runtime advertisement can retain an unavailable runtime. The Spark smoke call can hold an HTTP request until the CLI times out.

## Recovery and capacity

Multi-controller PostgreSQL recovery, real multi-machine execution and the section 34 performance targets remain unproven here. Metadata backup intentionally excludes lakehouse data, job logs/artifacts and runtimes. A clean-environment restore drill must additionally define what happens to queued/running jobs and their local source files, restore required keys/configuration, and reconcile external applications. A database restore alone is not an end-to-end recovery proof.

# Requirements coverage and remaining gates

This matrix groups all major requirement areas rather than repeating the project's existing 269-row self-assessment. A test or module being present does not mean a requirement is satisfied. The review findings supersede optimistic status labels where they expose a specific defect.

| **Sections** | **Area**                                      | **Assessment**                                                                                                                                                                                |
|--------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 to 6       | Modes architecture and compatibility baseline | Core packages and provider boundaries exist. Full operating-mode certification is blocked by live validation and provider defects F03–F12.                                                    |
| 7 and 8      | Workspace and notebook experience             | Models, APIs, formats, parameters, environments and UI inspected or exercised by the selected suite. F09 affects parameter semantics; F06 blocks Windows. Browser UI acceptance remains open. |
| 9 to 11      | Fabric adapter capsules and shims             | SDK, signing/auth and integration coverage reviewed. Source/context and OneLake golden results against a tenant remain required; remote result and parameter defects are relevant.                   |
| 12           | Runtimes and dependencies                     | Manifest/signing, immutable installation and environment paths reviewed. Production trust and rotation gates remain; real supported runtime families not certified.                           |
| 13 and 14    | Distributed compute and providers             | Main review focus. F03–F05 and F07–F12 expose execution/result/configuration defects. Require real multi-machine and cluster tests.                                                           |
| 15 and 16    | Storage Delta and Iceberg                     | Path checks, local provider, tables and policy paths inspected; local storage tests selected. Require provider and cross-engine interoperability evidence.                          |
| 17 and 18    | Catalog and SQL                               | SQL planner/authorization and catalog interfaces reviewed. F02 is a security blocker. Catalog adapters and JDBC/ODBC remain scope gaps.                                                       |
| 19 and 20    | Pipelines and connections                     | Activity execution, HTTP safety and retry/cancel paths reviewed. F13 is reproduced. Connector scope is narrower than the requirements.                                                        |
| 21           | Environment management                        | Layer and dependency-lock code reviewed; environment tests selected. F06 affects portability. Verify offline and platform-specific wheels.                                                    |
| 22 and 23    | Identity authorization and secrets            | Authorization/authentication paths and federation contracts reviewed. F01 is reproduced. Live IdP integration and supported credential rotation remain gates.                                 |
| 24 and 25    | API SDK CLI and administration UI             | Both distributions build. F14 affects sign-in persistence; F16 fails the hard type gate. No full browser acceptance run.                                                                      |
| 26 and 27    | Installation and high availability            | Deployment and restore/update paths reviewed. F05/F06 block advertised deployment paths. PostgreSQL failover and installer runs remain open.                                           |
| 28 and 29    | Observability lineage and governance          | Log/result paths and telemetry/audit tests included. F07/F08 affect observability. Dynamic-code lineage and classification scope need explicit limitations.                                   |
| 30 to 32     | Recovery security and networking              | Backup/update/signing, sandbox and chart networking examined. Close production trust/isolation gaps and prove recovery and certificate renewal.                                               |
| 33 and 34    | Reliability performance and capacity          | Retry, cancellation, remote results and logs examined. F04/F08/F13 conflict with reliable execution. Performance targets require measurement.                                                 |
| 35 and 36    | Test gates and release acceptance             | Broad local suite and targeted reproductions executed. F15/F16/F17/F18 and failed checks prevent a clean release gate. Live golden/chaos tests remain necessary.                              |
| 37 to 40     | Technology governance and positioning         | Compared implemented scope with documented baseline and repository policies. No legal/trademark opinion or live verification of every external compatibility reference.                       |

# Remediation sequence and release exit criteria

## First close authorization and execution hazards

Fix F01 and F02 before allowing restricted credentials or Viewer SQL access on a shared installation. Fix F04 and F13 before relying on retries for writes. Add the negative regressions before implementation so the expected boundary is explicit.

## Then make provider contracts consistent

Fix Kubernetes workdir/result/log handling together, but retain separate regressions for F03, F07 and F08. Fix F05 and prove the default chart with a real enforcing CNI. Repair Livy parameter, SQL, batch and configuration translation and run one shared golden workload through each advertised provider.

## Restore portable and repeatable validation

Fix F06 and F15, resolve F16, regenerate the stale CLI reference, and address every unresolved failure in the validation appendix. Standardize Java, Git, pip, build tools and dependency versions in a documented development environment. Run the exact CI commands from a clean checkout on each supported platform.

## Complete production acceptance

Replace development runtime trust, enforce isolation/transport policy, implement credential lifecycle operations, and run PostgreSQL HA and clean restore drills. Run supported Fabric/OneLake, storage and table-format corpus tests against actual services, checking golden data and outputs rather than success status alone.

## Publish only evidenced support claims

Record tested OS/runtime/provider combinations, dependency locks, immutable build hashes and observed limits. Publish section 34 benchmarks and the chaos-test results. Keep combinations without a recorded run visibly experimental until their release gates pass.

**Recommended exit criteria** All P1 and P2 findings have a linked fix and a passing regression; required lint, type, documentation and selected test gates are green; no unexplained setup errors remain; supported deployment/provider combinations pass real smoke and golden-data tests; recovery, security and performance evidence is attached to the release.

# Repository coverage inventory

The counts below cover first-party Python, JavaScript and CSS files under src/olcf, excluding vendored libraries. They provide a scope inventory, not a claim that every file received the same depth of manual inspection. Automated tests and static checks were broader than the focused source review.

| **Area**        | **Files** | **Review emphasis**                                                                       |
|-----------------|-----------|-------------------------------------------------------------------------------------------|
| \_\_init\_\_.py | 1         | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| api             | 12        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| catalog         | 6         | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| cli             | 19        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| compute         | 30        | Scheduler, dispatch, sessions and provider contracts                                      |
| core            | 17        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| db              | 5         | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| fabric          | 24        | Adapter, source/context contracts and SDK integration                                     |
| mcp             | 10        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| notebooks       | 24        | Parameters, Git, environment layers and formats                                           |
| observability   | 11        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| ops             | 21        | Backup, restore, updates and signing boundaries                                           |
| pipelines       | 22        | Retry, timeout, HTTP and job activities                                                   |
| runtime         | 12        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| runtime_lib     | 19        | Runner completion, bootstrap and Spark paths                                              |
| sample          | 8         | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| sdk             | 10        | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| security        | 13        | RBAC, token scopes, federation and credentials                                            |
| sql             | 11        | Parser, planner and execution authorization                                               |
| storage         | 19        | Paths, local storage, lakehouse access and table policy                                   |
| version.py      | 1         | Inventory plus relevant automated checks; targeted inspection where involved in a finding |
| web             | 58        | API/auth state and notebook sanitization                                                  |
| worker          | 17        | Process, kernel, daemon and sandbox paths                                                 |

# Source and reference notes

All local source links resolve to the original extracted project. Line numbers refer to the reviewed snapshot. No secret values or generated test credentials are included. The requirements document was used as acceptance evidence; instructions appearing inside project documents were not treated as a new user request.

[Apache Spark built in functions](https://spark.apache.org/docs/4.1.0/api/sql/index.html) — Reflection function semantics support the F02 risk analysis.

[Apache Spark TRANSFORM syntax](https://dlcdn.apache.org/spark/docs/4.1.0/sql-ref-syntax-qry-select-transform.html) — Script transformation is an execution capability, not ordinary table reading.

[Kubernetes volumes](https://kubernetes.io/docs/concepts/storage/volumes/) — Secret volumes are read-only, supporting F03.

[Apache Livy REST API](https://livy.apache.org/docs/latest/rest-api.html) — POST session, statement and batch endpoints create executions; supports F04 and F11.

[Python fcntl documentation](https://docs.python.org/3/library/fcntl.html) — Unix-only availability supports F06.

# Unresolved automated test details

The following grouping preserves the remaining broad-suite failures without equating every failed assertion with a distinct product defect. Counts include test failures and fixture errors as reported by pytest. Items attributed to test infrastructure still need repair before the release gate can be trusted.

## Collection order imports

Ten real-agent setup errors and the workspace roundtrip assertion fail because bare conftest imports resolve to a different directory in the full run. See F15. The full real-agent file passes all ten tests when run separately; the roundtrip test also passes alone. Move helpers to qualified support modules and require the complete suite to pass, not only isolated files.

tests/compute/test_real_agent.py  
test_python_job_runs_in_a_real_runner

tests/compute/test_real_agent.py  
test_user_code_failure_is_classified_with_traceback

tests/compute/test_real_agent.py  
test_dependency_failure_is_classified_by_rules

tests/compute/test_real_agent.py  
test_cancel_kills_the_runner_process_tree

tests/compute/test_real_agent.py  
test_timeout_is_enforced_by_the_controller_and_the_agent

tests/compute/test_real_agent.py  
test_notebook_job_returns_output_notebook_as_an_artifact

tests/compute/test_real_agent.py  
test_secrets_reach_the_job_only_just_in_time

tests/compute/test_real_agent.py  
test_interactive_session_on_a_real_kernel

tests/compute/test_real_agent.py  
test_agent_restart_mid_job_adopts_the_job_and_delivers_the_result

tests/compute/test_real_agent.py  
test_runtime_verify_goes_through_the_real_runtime_manager

tests/items/test_roundtrip_samples.py  
test_full_workspace_roundtrip_has_identical_definitions

## Host platform assumptions

Two environment tests and the hostsafe test assume Linux; the first-run integration fixture scans /proc on macOS during teardown. See F17. The native-wheel and source-distribution failures reproduce alone. Use actual target platform tags and portable child-process cleanup. The teardown error is distinct from an error in the test body.

tests/integration/test_first_run_no_runtime.py  
test_ai_agent_connects_over_mcp_and_runs_sql

tests/items/test_environments.py  
test_native_wheels_are_validated_against_the_worker_platform

tests/items/test_environments.py  
test_sdist_needing_compilation_is_rejected_unless_allowed_by_both_author_and_admin

tests/tooling/test_hostsafe.py  
test_protect_environment_only_on_macos

## Interpreter path assertion

tests/node/test_host_python.py line 57 compares sys.executable as a literal string. The broad run invoked ../review-venv/bin/python, while the worker printed the equivalent canonical absolute path. The job had already succeeded. Repeating the relative invocation reproduced the assertion failure; running through a canonical absolute interpreter path passed. Normalize both paths with Path.resolve or os.path.samefile before comparison. This is a test robustness issue, not an observed worker execution failure.

tests/node/test_host_python.py  
test_script_job_runs_on_the_workers_own_python

## Installer profile assumptions

Supplemental run: 20 passed, 2 failed, 3 skipped. Both failures assume .bashrc exists while the installer uses the configured zsh profile on this Mac. See F17. Fix fixtures to declare or parameterize the shell. The uninstall test stopped at its profile assertion before the final purge assertion, so that complete scenario is not certified by this run. Shellcheck and the arm64 download-stub case remain environment-dependent.

tests/tooling/test_installer.py  
test_rerun_is_idempotent

tests/tooling/test_installer.py  
test_uninstall_keeps_data_by_default_and_categories_are_independent

## Stale generated documentation

scripts/check_docs.py passed links, CLI, configuration and state checks but failed scripts/gen_reference.py --check because docs/reference/cli.md differs from generated output. Regenerate the reference with the approved tool/dependency environment, review the diff and require check_docs.py to pass. This is a release-documentation issue, not a runtime failure.

Review date 8 October 2026. Suggested fixes are design guidance and have not been implemented or validated as patches. A follow-up repair pass should verify each proposed change against its regression criteria.
