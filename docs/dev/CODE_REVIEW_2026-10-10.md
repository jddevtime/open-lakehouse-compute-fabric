<!-- Converted from Open_Lakehouse_Intelligence_Updated_Code_Review.docx (external review, 10 October 2026). Local file links were reduced to plain paths. The text is the reviewer's, unedited. -->

Version 0.1.0 • Intelligence, discovery, governance and lineage  
Prepared for Justin Dodrill • 10 October 2026

# Release assessment

The new subsystem is substantial and follows the intended modular
direction, but this build should not be described as production-ready or
as “everything works.” The review found concrete defects in sampling
controls, asset identity, SQLite merge auditing, scan completeness and
lineage caching, plus problems in release verification. Several
reproduced failures sit outside the scenarios covered by the existing
tests.

This report records 16 findings with source references, observed
behavior, proposed repairs and acceptance tests. P1 findings are
high-priority release blockers for the affected feature; P2 findings are
important correctness or verification defects. No P0 emergency is
asserted. Recommendations are not completed fixes.

**What already works** The server and Fabric SDK release artifacts build
and install successfully in clean temporary environments. The 14
intelligence browser tests pass with the documented harness
configuration corrections. Lint, required CLI/SDK type checks and
documentation checks pass. The final validation table distinguishes
these successes from failed and unexecuted checks.

**What changed in this review** No application, test, workflow or
documentation source in the supplied build was intentionally edited.
Review scripts, temporary environments, test evidence and this report
were created outside the build. Test/build caches may have been
generated. No commit, GitHub publication, deployment, email or external
data scan was performed.

**Contact** The latest build already names jdsql@hotmail.com in
SECURITY.md; repository metadata names
jddevtime/open-lakehouse-compute-fabric. These are observed settings,
not confirmation that remote repository controls or publishing
identities are configured.

**Source reviewed**
/Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0

**Snapshot** 1,366 non-cache files; 799 Python files, including 399
application modules, 370 test Python files and 66 intelligence modules.
Manifest SHA-256:
acdc0c2c3c65613bdc24c24e3c46275f7aadc736e314ae80c7d92e3e74c3261e.

The review combined direct code tracing, adversarial local
reproductions, automated tests, package builds and official
API-documentation checks. It is a risk-based engineering review rather
than a proof of every possible execution path. Vendored browser
dependencies were inventoried, not manually audited line by line. Fabric, PostgreSQL, Kubernetes and other environments remain
explicit release gates.

# Finding index

| **ID** | **Priority** | **Problem**                                                                                   |
|--------|--------------|-----------------------------------------------------------------------------------------------|
| F01    | P1           | A column sample bypasses its parent table’s sampling ban                                      |
| F02    | P1           | Automatic value profiling omits asset policies and approved classifications                   |
| F03    | P2           | A column sample expands to every column when schema metadata is missing                       |
| F04    | P1           | Mapped Fabric workspaces can collapse distinct tables into one asset                          |
| F05    | P1           | Asset merge auditing creates a SQLite writer deadlock and drops the audit                     |
| F06    | P2           | Partial Fabric scans advance the watermark past failed workspaces                             |
| F07    | P2           | Short-interval scans send an invalid Power BI modifiedSince value                             |
| F08    | P2           | Pagination caps silently return incomplete inventories as complete                            |
| F09    | P2           | The analysis cache ignores changes in resolution context                                      |
| F10    | P1           | A parse-error scan can succeed and retire previously known lineage                            |
| F11    | P1           | The browser CI job can pass while skipping its browser tests                                  |
| F12    | P2           | The E2E harness can generate invalid overlapping Spark port ranges                            |
| F13    | P2           | New synthetic fixtures fail the blocking secret-scan gate                                     |
| F14    | P2           | The OneLake byte cap is checked after the entire response is buffered                         |
| F15    | P1           | The new performance test breaks cross-platform collection and memory accounting               |
| F16    | P2           | The first-run notebook test still expects a fallback after an explicit host-runtime selection |

Detailed findings follow the verification record and architecture
assessment. Their IDs apply to this report, not to the earlier review
documents. Source line numbers refer to this exact extracted build.

# Verification record

| **Check**                | **Observed result and limits**                                                                                                                                                                                                                                                                                                           |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Intelligence suite       | Initial completed run: 1,799 passed, 1 failed and 34 setup errors (53 deselected), in 756.49 seconds. The 34 setup errors came from three runtime modules run without JAVA_HOME; their corrected-environment whole-file rerun passed all 35 tests in 89.73 seconds. The remaining failure is the macOS memory-unit defect F15.           |
| Broader regression suite | Four completed shards: 3,741 passed, 2 failed, 0 errors and 18 skipped (3,761 executed/skip outcomes). The S3 diagnostic assertion passed in a short-path whole-file rerun (10 passed); the stale first-run fallback expectation failed again (1 failed, 6 passed; F16). These are the original run counts, not an all-green claim.      |
| Browser tests            | Initial run: 14 setup errors caused by overlapping generated Spark port ranges. Rerun: 14 passed in 102.31 seconds with Playwright-resolved Chromium and both master port spans set to 1. Real local control plane and three worker agents; no real Fabric tenant.                                                                       |
| Targeted reproductions   | Confirmed sampling-policy bypasses, expanded column output, mapped-workspace identity collision, a 31.119-second merge with no audit row, shared SQLite blocking, partial watermark loss, invalid recent modifiedSince, silent page truncation, stale context cache, parse-error success/edge retirement and late HTTP size enforcement. |
| Release build            | scripts/build_release.sh completed: server and SDK wheels and source archives, artifact gates, separate clean wheel install tests, initialization, SBOMs, notices and SHA256SUMS verification. This was a local build; no signing or publication occurred.                                                                               |
| Lint and required types  | Ruff: all checks passed. Required mypy scope: 36 CLI/SDK source files passed. This is not a whole-package type-clean claim.                                                                                                                                                                                                              |
| Documentation            | check_docs.py passed links, CLI, configuration, states and generated-document checks across 103 Markdown files. Header check: 789 files passed. Module/class docstring thresholds passed: 399/399 modules and 881/882 classes.                                                                                                           |
| Deployment checks        | 106 passed, 5 skipped in a separate run with Helm 3.16.2 and kubeconform 0.6.7 available. Docker/systemd execution was skipped. This overlaps the broad test selection.                                                                                                                                                                  |
| Workflow syntax          | Actionlint 1.7.7 passed when given the workflow files explicitly; the extracted build has no .git directory. Syntax validation does not establish that a workflow exercises its intended tests.                                                                                                                                          |
| Release identity         | Identity preflight passed; it still reports 13 files containing generic deployment/verification templates as nonblocking notes. Verify the published quickstart and image URLs before announcement.                                                                                                                                      |
| Secret scan              | Gitleaks 8.24.3 reported three synthetic test-fixture findings (F13). The directory scan is not clean. No Git history was available for history scanning.                                                                                                                                                                                |
| Dependency advisories    | Resolved all runtime extras for Python 3.13 on Linux x86_64, macOS arm64 and Windows x86_64; pip-audit reported no known vulnerabilities for all three resolved sets. OSV, CodeQL, container images, JVM/JAR contents and vendored JavaScript were not independently audited here.                                                       |
| Source integrity         | All 1,366 inventoried source files retain their original hashes; no non-cache additions/deletions/changes. All 60 first-party UI JavaScript files passed Node module-syntax checking.                                                                                                                                                    |

**Test selection** Python 3.12.12 on macOS arm64; Java 21 and bundled
Node available. The non-browser suite selected not spark and not e2e and
not live and not postgres. The broader regression run was partitioned by
whole test module into four isolated processes; intelligence tests were
run separately. An earlier serial run was stopped and is excluded from
aggregate results. Overlapping reruns are not added as unique coverage.

**Skips and exclusions** The broad-run skip reasons are listed below.
Spark-, PostgreSQL-, live-service- and browser-marked cases were
excluded from that selection. Browser intelligence tests were run
separately. A separate deployment run with Helm/kubeconform available
passed 106 tests and skipped 5 Docker/systemd checks. These reruns
overlap the broader suite and must not be summed as unique tests.

**Runtime-lineage environment correction** The first intelligence
invocation did not export JAVA_HOME, although Java was available on
disk. The real worker rejected job execution; related fixture failures
cascaded into 34 setup errors. Re-running test_fabric_runtime_link.py,
test_failure_isolation.py and test_runtime_lineage.py with Java
configured produced 35 passes. No production repair was inferred from
this environment mistake.

**Memory test defect — unresolved**
test_memory_stays_bounded_and_repeated_queries_do_not_grow_it fails on
macOS because it multiplies an already-byte-valued peak by 1,024. Its
top-level Unix-only resource import also prevents Windows collection.
See F15; the repository test remains unfixed.

**S3 diagnostic assertion — path dependent** test_s3_target_end_to_end
failed only in the long review temporary directory: the bounded
diagnostic string was truncated before the complete NoSuchBucket token.
All 10 backup-target tests passed with a shorter temporary root.
Preserve a structured provider error code independent of path-rich
display text and assert that code rather than searching a truncated
message.

**First-run expectation — unresolved**
test_sample_notebook_02_runs_on_host_python failed in both the broad run
and a fresh seven-test file rerun. The job succeeds, but the test
incorrectly expects fallback after the sample explicitly selects host
Python. See F16; subsequent notebook-output assertions in that test are
not reached.

**Broad-run skips** 2 × shellcheck is not installed in this sandbox; 1 ×
the real msvcrt branch; 6 × the cached Spark test runtime
(/tmp/olcf-test-runtime) is not available; build it with
tests/node/runtime_fixture.py; 2 × set KUBECONFORM or put kubeconform on
PATH to validate the manifests against the Kubernetes schemas; 1 × helm
is not installed here; CI runs helm lint and helm template; 4 × docker
is not installed; 1 × systemd-analyze is not available; 1 ×
systemd-analyze not installed

# Architecture assessment

The separation is real at the package/API level. Compute emits dataset
events; the intelligence inbox processes them in the background.
Discovery uses its own connectors and secrets. Governance accepts
observe mode and rejects enforce. The module can be omitted, and tests
cover compute without intelligence and intelligence without compute.
These are useful foundations to preserve during repairs.

| **Requirement**                         | **Assessment**                                                                                                                                                                                                                                                                                                                                                                         |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Compute continues if intelligence fails | Partially met. Subscriber exceptions are contained and waits are bounded, but event persistence and graph writes share a database and the same process. A held intelligence SQLite transaction delayed core event publication by 2.025 seconds. F05 demonstrates a concrete 31-second self-blocking path. The 0.5-second claim covers subscriber waiting, not total job/event latency. |
| No circular runtime dependencies        | The code uses event/API boundaries and has import-graph tests. Keep this guard. Resource and database coupling remain even without circular imports.                                                                                                                                                                                                                                   |
| Stable shared asset identity            | Registry and aliases are implemented, including placeholders and merge hooks. F04 violates identity separation for valid many-to-one workspace mappings. Name aliases must not override conflicting authoritative IDs.                                                                                                                                                                 |
| Observe by default                      | Implemented; policy enforcement is explicitly refused. Keep enforcement separate and opt-in if added later. Observation is not authorization to sample data.                                                                                                                                                                                                                           |
| Sampling separately authorized          | Implemented in principle with separate permissions, policies and Fabric sampling identity, but F01/F02 undermine the policy boundary. Default-off alone is insufficient once broad allows coexist with narrower denies.                                                                                                                                                                |
| Honest confidence and gaps              | Confidence/evidence/gap models are implemented. F06–F10 show stale or incomplete results can still be treated as current/successful. Runtime “verified” means an instrumented operation reported activity; lazy read plans do not prove rows were materialized.                                                                                                                        |
| Independent discovery path              | Connector and scanner import exist. Fabric APIs, Power BI metadata and OneLake log reads are distinct from the compute adapter, but no real-tenant validation was performed. Workspace-visible discovery must not be marketed as guaranteed tenant-wide coverage.                                                                                                                      |
| Modular deployment                      | Local/browser tests exercise modules without requiring a JVM for intelligence. A separate process/storage boundary is still needed if strict fault and memory isolation is a deployment requirement.                                                                                                                                                                                   |

# Detailed code findings

## F01 — A column sample bypasses its parent table’s sampling ban

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/discovery/sampling.py lines 309 to
315](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/sampling.py)

**Problem** When the caller requests a column asset, sample() finds the
parent table but resolves the policy with the column’s asset ID. The
table’s asset-level policy is never considered. A broader
workspace/source/global allow therefore defeats a table-level max_rows=0
ban; a stronger table masking rule can also be bypassed.

**Verification** Executed with the real SQLite SamplingPolicyStore and
registry, a global five-row/unmasked allow and an explicit table ban.
Sampling the table raised PolicyDenied. Sampling its city column
returned Oslo under the global policy. The data reader was a local Arrow
stub, so this proves policy selection without accessing any real data.

**Impact** Release blocker for deployments enabling sampling; the caller
still needs intelligence.sample, but can exceed the configured
data-reading policy.

**Recommended fix** Resolve a canonical parent-table policy before any
read. Define and enforce column inheritance explicitly: a column must
inherit the table restriction unless an authorized, explicit column
override is part of the policy model. Apply row limits, masking and
classification exclusions consistently. Do not merely replace the ID if
column-specific policy support must remain.

**Acceptance test** Global allow + table deny + column request must deny
before invoking the reader. Repeat for source/workspace allows, disabled
policies, stricter table row limits and masking, and any explicitly
supported column override.

## F02 — Automatic value profiling omits asset policies and approved classifications

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/discovery/local.py lines 437 to
451](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/local.py)

**Problem** The value-profiling path resolves only workspace_id and
source_id. It cannot see a table-specific deny because no asset_id is
supplied. Its excluded-column list is derived from heuristic sensitive
names, rather than the governance classification providers used by the
interactive sample endpoint.

**Verification** Executed \_value_profile with the same real policy
store as F01: table sampling was explicitly disabled, yet the storage
reader was called for city and amount and the asset acquired
value_profile metadata under the global allow. Code inspection
separately confirms that this path never consults the registered
governance classification providers.

**Impact** Release blocker when value_profiling is enabled. Storing only
aggregates does not make an unauthorized read acceptable.

**Recommended fix** Centralize authorization, effective policy
resolution and classified-column exclusion in one data-read policy
service shared by sampling and profiling. Resolve an existing table by
its authoritative alias before reading; support newly discovered assets
without weakening an existing deny. Preserve raw in-memory profiling
where needed, but require the same access and exclusion decisions and
audit the authorized operation.

**Acceptance test** A rescanned table with max_rows=0 must cause zero
storage reads. A non-obviously named column with an approved governance
classification must be excluded. A failed classification lookup must
fail closed, and aggregate-only output must still contain no raw values.

## F03 — A column sample expands to every column when schema metadata is missing

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/discovery/sampling.py lines 321 to
353](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/sampling.py)

**Problem** only_column narrows the declared schema before the read. If
the parent has no recorded columns, the fallback uses every actual
reader column and does not reapply only_column. This is a realistic
state after metadata permission failures or incomplete schema discovery.

**Verification** Removed the table’s recorded column list in the
isolated SQLite graph and requested only its city column asset. The
response contained both city and amount. The reader was the same small
Arrow stub used in F01.

**Impact** Unexpected data disclosure within a table and an API contract
violation. Fix alongside F01.

**Recommended fix** Carry the requested column projection through both
the declared-schema and reader-schema paths. Validate it against the
actual schema and return only that column. If it is absent, return an
explicit error rather than broadening the result. Reapply exclusions to
the final projection before serialization.

**Acceptance test** With attrs.columns absent or empty, requesting a
city column asset must return exactly city. Cover a missing physical
column, classified requested column, mixed-case names and explicit
columns parameters.

## F04 — Mapped Fabric workspaces can collapse distinct tables into one asset

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/registry.py lines 69 to
80](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/registry.py)

[src/olcf/intelligence/registry.py lines 480 to
505](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/registry.py)

[src/olcf/intelligence/registry.py lines 528 to
571](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/registry.py)

**Problem** Fabric references receive both the local workspace scope and
their remote Fabric scope. Derived qualified-name aliases use both. If
two remote workspaces map to one OLCF workspace and both contain
Silver.orders, the shared local qualified-name alias matches the first
table despite their different Fabric identities and different lakehouse
parents. ensure() accepts that hit and attaches the second identity to
it.

**Verification** Executed two scans with remote-a and remote-b, distinct
Fabric lakehouse/table aliases, the same local workspace and matching
display names. Both table references resolved to the same ast\_ ID; its
parent changed to the second lakehouse. No Microsoft service or network
call was involved.

**Impact** Release blocker: lineage and governance can be attributed to
the wrong physical dataset.

**Recommended fix** Treat tenant/workspace/item/table identities as
authoritative. Qualified names are resolution hints and must not merge
real assets with conflicting authoritative identities. Keep ambiguous
local qualified names multi-valued or unresolved, and select by the
caller’s remote scope. If many-to-one workspace mapping is unsupported,
reject it explicitly rather than accepting corrupt identities.

**Acceptance test** Scan two remote workspaces with identical names into
one local workspace and verify distinct asset IDs, parents, columns,
lineage, ownership and classifications. Also cover cross-tenant mappings
and rename/rescan stability.

## F05 — Asset merge auditing creates a SQLite writer deadlock and drops the audit

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/registry.py lines 586 to
636](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/registry.py)

[src/olcf/intelligence/module.py lines 101 to
105](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/module.py)

[src/olcf/security/audit.py lines 75 to
94](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/security/audit.py)

**Problem** The registry invokes its merge notification while the
intelligence.graph write transaction is still open. The installed
notification calls AuditLog.record(), which opens a second write
transaction on the same SQLite database. That second writer waits for
the first transaction, but the first cannot commit until the callback
returns. The caught timeout allows the merge to finish without its audit
record.

**Verification** Used the actual Database, AssetRegistry and AuditLog
against a temporary on-disk SQLite database. Merging two real notebook
assets took 31.119 seconds; querying intelligence.asset_merged returned
zero audit rows. The captured log contains the database-locked failure.
A separate held-graph-lock probe delayed EventBus.publish by 2.025
seconds despite no subscribers.

**Impact** Release blocker for the default SQLite deployment. It also
exposes a shared-database limitation in the claimed subsystem failure
isolation.

**Recommended fix** Record merge audit data within the existing
transaction through a connection-aware audit API, or commit a
transactional outbox and deliver after commit. Preserve audit-chain
serialization without opening a competing writer. Do not solve this by
silently dropping audit records or merely shortening busy_timeout. Move
notifications outside registry critical sections.

**Acceptance test** A real-asset merge on SQLite must complete promptly
and produce exactly one verifiable audit record. Exercise nested child
merges, concurrent merges and rollback. While merging, run job state
writes and event publication and assert latency and correctness.

## F06 — Partial Fabric scans advance the watermark past failed workspaces

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/discovery/scans.py lines 290 to
302](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/scans.py)

[src/olcf/intelligence/discovery/fabric.py lines 541 to
544](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/fabric.py)

[src/olcf/intelligence/discovery/fabric.py lines 597 to
607](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/fabric.py)

**Problem** A partial scan is considered successful for checkpoint
persistence. The Fabric connector saves a new global watermark and
records every listed workspace as known even when reading one failed. A
subsequent incremental scan selects only modified or unknown workspaces,
so a failed workspace can disappear from the retry set after permissions
or connectivity recover.

**Verification** Executed the actual scan-finalization and
incremental-selection methods with controlled state. A partial scan
persisted ok=True, its new watermark and the failed workspace in
state.workspaces. The next modified-workspaces response was empty, and
the next selection was also empty. This is a deterministic state-machine
reproduction, not a live-tenant test.

**Impact** Metadata and lineage can remain stale until a full scan,
undermining incremental recovery.

**Recommended fix** Persist a durable failed/incomplete workspace retry
set and union it into each incremental selection, or retain
per-workspace successful checkpoints. A global watermark must not imply
successful inspection of every workspace. Preserve cancellation and
permission-failure distinctions and back off retries without abandoning
them.

**Acceptance test** Fail one workspace on a partial scan, restore access
without changing its content, return no modified IDs, and require it to
be retried and repaired. Include scanner-phase failures and a restart
between scans.

## F07 — Short-interval scans send an invalid Power BI modifiedSince value

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/discovery/fabric.py lines 582 to
605](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/fabric.py)

**Problem** The incremental query subtracts only two minutes from the
previous scan watermark. Microsoft documents a minimum age of 30 minutes
for modifiedSince. A scan five minutes after the previous one therefore
asks for an unsupported timestamp seven minutes old. On API failure the
connector falls back to inspecting every workspace and records a
blocking gap.

**Verification** Executed \_modified with a 12:00 watermark and a 12:05
scan start. It requested 11:58. Microsoft Learn explicitly requires a
timestamp between 30 minutes and 30 days before the current time. The
local test server does not establish acceptance by the service itself.

**Impact** Configured frequent scans can repeatedly degrade into
expensive partial scans.

**Recommended fix** Use a safe delayed watermark with a documented
overlap, for example never newer than now minus 30 minutes plus a small
safety margin, while retaining the lower-bound history needed to avoid
missed changes. Respect the endpoint’s 30 requests/hour limit across
sources and replicas. Test the boundary using a fake clock.

**Acceptance test** Exercise 1-, 5-, 15- and 60-minute scan intervals,
clock skew and a watermark near the 30-day boundary. Assert every
outgoing modifiedSince is valid and that delayed changes are eventually
scanned without a perpetual full-scan fallback.

External contract: [Microsoft Learn: Get Modified
Workspaces](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-modified-workspaces)

## F08 — Pagination caps silently return incomplete inventories as complete

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/fabric/rest.py lines 258 to
307](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/fabric/rest.py)

[src/olcf/fabric/rest.py lines 403 to
415](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/fabric/rest.py)

**Problem** Workspace, item, table and OneLake listing methods stop at
max_pages even if the final response still has a continuation token.
They return ordinary results without a truncation signal. The scan
runner can then label a full scan succeeded and sweep previously known
objects that were merely beyond the cap. A repeated continuation token
has the same eventual failure mode.

**Verification** The real list_lakehouse_tables method, with max_pages=1
and a response containing one table plus continuationToken=next,
returned that one table after one request without raising or marking
incompleteness. The production default cap is 2,000 pages; the smaller
cap makes the same branch inexpensive to reproduce.

**Impact** Large or anomalous upstream inventories can cause false
removals.

**Recommended fix** Raise a typed incomplete-inventory error or return
an explicit completeness flag when a cap is exhausted with continuation
remaining. Detect repeated tokens. Connect this status to a blocking gap
and suppress removal sweeps for the affected scope. Retain already
received data as partial results where useful.

**Acceptance test** A full rescan with an outstanding token at the cap
must be partial and must not remove an existing unseen asset. Cover
every paging method, repeated tokens, exactly-full final pages, empty
pages with tokens and cancellation.

## F09 — The analysis cache ignores changes in resolution context

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/engine.py lines 416 to
424](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/engine.py)

[src/olcf/intelligence/discovery/local.py lines 296 to
329](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/local.py)

**Problem** Analysis is skipped solely on definition etag and analyzer
version. Resolution also depends on context such as the workspace
default lakehouse, schemas, remote identity and model bindings. These
can change without changing the saved query or notebook etag. Cached
edges are kept alive against their former target.

**Verification** Analyzed SELECT \* FROM orders with an unchanged
definition etag and default lakehouse alpha. Repeated with default
lakehouse beta: analysis_skipped was 1 and the active read edge still
pointed to alpha. Forcing analysis moved the read edge to beta. Both
lakehouses and tables existed in the real temporary graph.

**Impact** Impact analysis and root-cause answers can confidently
reference an obsolete dataset.

**Recommended fix** Include a deterministic fingerprint of all
lineage-relevant context in the cache key, or track dependencies and
invalidate on relevant metadata changes. Include schema versions for
wildcard expansion and scope/default bindings. Version the stored cache
schema and provide migration or safe invalidation.

**Acceptance test** Change the workspace default lakehouse without
editing the query and require its lineage to refresh. Repeat for
schemas, model bindings, renamed containers and remote workspace
mapping; irrelevant volatile context should not force unnecessary
analysis.

## F10 — A parse-error scan can succeed and retire previously known lineage

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[src/olcf/intelligence/engine.py lines 425 to
451](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/engine.py)

[src/olcf/intelligence/discovery/scans.py lines 266 to
278](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/intelligence/discovery/scans.py)

**Problem** Pure analyzers communicate failure using Facts.gaps rather
than exceptions. The engine records these gaps but does not convert
blocking analysis failures into errors. The runner counts context gaps
and apply errors, not the blocking gaps inside analyzed facts.
\_finish_analysis also retires old edges immediately when the failed
analysis returns no replacement edges.

**Verification** After successful analysis of a query, supplied an
updated malformed SQL definition. The analyzer returned parse_error;
ApplyStats had gaps=1 and errors=0. The runner’s outcome was succeeded,
and previously active lineage edges were already retired. This used the
actual SQL analyzer, engine and SQLite graph.

**Impact** Release blocker for trustworthy governance and impact
analysis: failure can be presented as successful absence.

**Recommended fix** Carry explicit completeness and blocking-failure
status from each analyzer into ApplyStats and the scan outcome. Retain
last-known-good edges when an analysis is incomplete, marked stale with
a gap and their original evidence version. Retire edges only when the
relevant definition was completely and successfully reanalyzed. Apply
the same rule to full-scan sweeping.

**Acceptance test** Start with valid lineage, then introduce a parse
failure or analysis cap. Require partial status, a visible gap and
retained stale lineage rather than a false clean graph. Repair the
definition and verify that genuinely removed edges are then retired.

## F11 — The browser CI job can pass while skipping its browser tests

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[tests/e2e/conftest.py lines 34 to
35](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/e2e/conftest.py)

[tests/e2e/conftest.py lines 97 to
105](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/e2e/conftest.py)

[.github/workflows/ci.yml lines 212 to
238](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/.github/workflows/ci.yml)

**Problem** The fixture defaults to a specific
/opt/pw-browsers/chromium-1194 Linux executable and skips when it is
absent. The GitHub workflow installs Playwright’s current Chromium into
its normal cache and does not supply OLCF_E2E_CHROMIUM. A fresh hosted
runner does not acquire the fixture’s hardcoded path from that
installation command. Browser-dependent tests can therefore be skipped
while the job is green, assuming preceding stack setup succeeds.

**Verification** Verified the fixture and workflow together and resolved
the installed browser through Playwright locally: it was revision 1243
under the isolated review cache, not the hardcoded revision/path. The
review browser run explicitly used that resolved path. Official
Playwright documentation confirms per-version browser binaries and
platform cache locations.

**Impact** Release verification blocker: green CI currently does not
demonstrate the intended UI coverage.

**Recommended fix** Launch pw.chromium with Playwright’s default
executable resolution. Keep an explicit executable override only when
requested. In the required CI browser job, fail if Chromium is missing
or zero browser tests execute; upload a JUnit report and traces and gate
on unexpected skips.

**Acceptance test** On a clean hosted runner, install Chromium, execute
at least one real navigation assertion, and assert the expected
browser-test count. Remove the executable and require a failing job
rather than a skip-only success.

External contract: [Playwright: browser binaries and
installation](https://playwright.dev/python/docs/browsers#managing-browser-binaries)

## F12 — The E2E harness can generate invalid overlapping Spark port ranges

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[tests/e2e/stack.py lines 130 to
145](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/e2e/stack.py)

[src/olcf/core/config.py lines 94 to
99](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/core/config.py)

**Problem** The harness independently chooses free single ports for the
master RPC and UI bases but leaves both configured spans at 16. Two free
ports can be fewer than 16 apart, which violates the production
configuration validator and prevents the control plane from starting.
The fixture then waits 90 seconds and reports connection refused for
every test.

**Verification** The first attempted run of the 14 intelligence browser
tests produced 14 setup errors. Its control-plane log identified
overlapping master_rpc and master_ui ranges. The verification rerun set
both spans to 1 in the test environment; no repository source was
changed.

**Impact** A reproducible test-harness defect that can obscure actual UI
failures and make CI flaky.

**Recommended fix** Allocate and reserve non-overlapping ranges for the
full configured spans, or use single-port spans for a suite that needs
only one runtime family. Check all role ranges. Detect subprocess exit
and surface the startup log immediately; put startup itself inside
guaranteed cleanup.

**Acceptance test** Use a deterministic allocator returning adjacent
ports and verify the harness still generates valid settings. Repeat
startup many times and require prompt, informative failure if the server
exits.

## F13 — New synthetic fixtures fail the blocking secret-scan gate

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[tests/compute/test_dataset_events_jobs.py lines 43 to
47](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/compute/test_dataset_events_jobs.py)

[tests/intelligence/discovery/test_scanner_import.py lines 100 to
105](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/intelligence/discovery/test_scanner_import.py)

[tests/intelligence/system/test_deployment_shapes.py lines 181 to
186](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/intelligence/system/test_deployment_shapes.py)

**Problem** Three new negative-test fixtures contain deliberately
credential-shaped strings without the narrow per-line exceptions used
elsewhere. The repository’s blocking gitleaks directory scan reports
them. Their surrounding tests establish that these are synthetic
redaction/authentication inputs, not discovered production credentials.

**Verification** Gitleaks 8.24.3 scanned the current tree with the
repository configuration and reported exactly three findings:
generic-api-key at test_dataset_events_jobs.py:44, jwt at
test_scanner_import.py:102 and generic-api-key at
test_deployment_shapes.py:185. No credential values are reproduced in
this report. The workflow pins 8.24.2, which should also be run before
release.

**Impact** Release gate failure, not evidence of a real credential
breach. Historical Git scanning still requires an actual repository
checkout.

**Recommended fix** Add justified per-line gitleaks:allow annotations
for these synthetic inputs, or narrowly fingerprint the fixtures if a
multiline annotation cannot be scoped correctly. Preserve the
negative-test strings where their shape is what the test exercises. Do
not exclude whole test directories.

**Acceptance test** Run the workflow-pinned scanner and require a clean
tree. Add a separate temporary, unallowlisted credential-shaped string
and confirm the scanner still detects it; remove the temporary fixture
afterwards.

## F14 — The OneLake byte cap is checked after the entire response is buffered

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[src/olcf/fabric/rest.py lines 130 to
140](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/fabric/rest.py)

[src/olcf/fabric/rest.py lines 417 to
423](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/fabric/rest.py)

**Problem** onelake_read advertises a bounded small-file read, but
\_request uses client.request(), which consumes the response body before
returning. The length check occurs after that allocation. Oversized
Delta log/checkpoint responses can therefore consume much more memory
than the configured cap inside the control-plane process.

**Verification** An httpx MockTransport streamed eight 1-KiB chunks into
the production client with max_bytes=1 KiB. All 8 KiB were consumed
before FabricApiError was raised. This demonstrates late enforcement; it
is not an out-of-memory load test.

**Impact** Resource-isolation risk: an unexpectedly large metadata
response can pressure memory shared with compute coordination.

**Recommended fix** Use a streaming response path and stop once the
decoded byte limit is exceeded. Reject excessive Content-Length early
but do not trust it as the sole guard. Close the stream
deterministically, bound decompressed content, and integrate
cancellation and retry behavior. Apply equivalent limits before
definition/scanner JSON materialization where appropriate.

**Acceptance test** Use an oversized chunked body without Content-Length
and require reading to stop at the cap plus at most one chunk. Cover
compressed content, false Content-Length, cancellation and a small valid
file.

## F15 — The new performance test breaks cross-platform collection and memory accounting

**Priority** P1 • Confirmed code finding • Suggested changes not applied

[tests/intelligence/system/test_performance.py lines 28 to
37](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/intelligence/system/test_performance.py)

[tests/intelligence/system/test_performance.py lines 243 to
248](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/intelligence/system/test_performance.py)

**Problem** The module imports the Unix-only resource package at
collection time, so Windows cannot collect the suite that includes this
file. The memory assertion also multiplies ru_maxrss by 1,024
unconditionally, although macOS already reports bytes. That inflates the
observed peak by a factor of 1,024 and causes a false failure.

**Verification** The macOS intelligence suite failed the peak-memory
assertion with a reported 930,512 MB. Its raw ru_maxrss corresponds to
approximately 909 MiB, below the 1.5-GiB threshold. The Windows
collection failure is established by the unconditional import and the
official Python availability contract; no Windows machine was used in
this review.

**Impact** Release verification blocker for the advertised
cross-platform CI matrix; the observed macOS failure is not evidence
that the application used hundreds of gigabytes.

**Recommended fix** Use a platform-aware memory measurement helper, with
bytes as its explicit output unit. Avoid importing resource
unconditionally on Windows; use a suitable Windows peak-working-set API
or a clearly documented alternative. Run memory-bound workloads in a
fresh process so a whole-suite historical peak does not contaminate the
benchmark.

**Acceptance test** Require successful collection on Windows and correct
byte-normalized measurements on Linux and macOS. Verify known unit
conversions independently and execute the same isolated scale workload
on each supported OS.

External contract: [Python documentation: resource
availability](https://docs.python.org/3/library/resource.html)

## F16 — The first-run notebook test still expects a fallback after an explicit host-runtime selection

**Priority** P2 • Confirmed code finding • Suggested changes not applied

[tests/integration/test_first_run_no_runtime.py lines 69 to
79](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/tests/integration/test_first_run_no_runtime.py)

[src/olcf/sample/notebooks.py lines 370 to
386](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/sample/notebooks.py)

[src/olcf/compute/jobs.py lines 449 to
453](file:///Users/aimac/Downloads/latest_build/Open_Lakehouse_Fabric_Compute_Extender-0.1.0/src/olcf/compute/jobs.py)

**Problem** The sample manifest now explicitly requests the host-python
family for 02_Retail_Explore_Python. The first-run integration test
still requires a runtime_fallback timeline event. Directly selecting the
requested host runtime is not a fallback, so the assertion conflicts
with the current sample configuration.

**Verification** The broad run and a fresh whole-file rerun both failed
the same assertion. In the rerun, the notebook succeeded on
olcf-host-python with the expected runtime fingerprint; the other six
first-run tests passed. Source tracing shows fallback events are emitted
only when runtime resolution actually produced fallback metadata.

**Impact** A repeatable release-gate failure caused by an outdated
expectation, not a demonstrated notebook execution failure.

**Recommended fix** Update this test to assert explicit host-runtime
selection and retain the notebook-output checks that currently occur
after the failing assertion. Keep a separate test that requests an
unavailable compatible family and verifies genuine fallback events. Do
not add a misleading production fallback event merely to satisfy the
stale assertion.

**Acceptance test** Run the complete first-run file: all seven tests
should pass, including the executed-output checks. Separately verify a
true missing-runtime fallback and a pinned runtime that must not fall
back.

# Recommended implementation sequence

**1. Restore the data-reading boundary** Fix F01–F03 together through
shared policy resolution. Keep interactive samples and automated
profiling separate operations with identical permission/exclusion
decisions. Add reader-spy tests proving denied paths perform zero data
reads.

**2. Protect identity and transactions** Fix F04/F05 before expanding
scans across tenants or mapped workspaces. Add migrations or repair
tooling for already-conflated identities and missing audit records;
splitting a merged graph requires provenance review, not just a code
patch.

**3. Make incomplete scans safe** Fix F06–F10 as one completeness model:
per-scope retry state, supported watermark window, explicit inventory
truncation, context-aware cache keys and last-known-good lineage. A scan
with unresolved collection/parse failures must never authorize deletion
based on absence.

**4. Repair verification gates** Fix F11–F13 and F15–F16 and require
actual browser execution, deterministic test ports and a clean
workflow-pinned secret scan. Re-run the affected files and the full
immutable release suite; inspect skipped tests instead of treating a
zero exit code as sufficient.

**5. Bound resources and test isolation** Fix F14. Validate queue
pressure, DB lock contention, large definitions and subscriber stalls
under simultaneous job traffic. Publish a precise latency/loss contract;
add durable outbox/replay or independent storage/processes where the
deployment requires it.

**6. Validate the release candidate externally** Run clean
Linux/macOS/Windows installations and PostgreSQL, Spark,
Fabric/Entra/OneLake and cluster acceptance tests. Build the final
candidate from a fixed commit, execute GitHub gates, verify
signatures/checksums and only then publish the scoped beta.

# Additional recommendations and verification gaps

## R01 — Define failure isolation at the resource boundary

Exception isolation is not enough for the requirement that a broken
intelligence subsystem never stops compute. Specify database lock/pool,
CPU, memory and queue budgets. The event bus persists before its
subscriber wait budget begins. Consider a bounded nonblocking handoff
and a separate intelligence store/process; retain a shared stable-ID
contract through APIs rather than a shared lock. Acceptance: sustained
job submissions and completions while graph writes stall, its connection
pool is exhausted and oversized metadata arrives.

## R02 — Run a least-privilege real-tenant acceptance matrix

Use a dedicated test tenant/workspace set and separate metadata/sampling
identities. Verify list pagination, 202 definition polling, throttling,
token refresh, schema-enabled lakehouses, OneLake log/checkpoint
variants and scanner permission failures. Compare discovered assets
against an independently known inventory. Test an admin-scanner-visible
workspace absent from the metadata identity’s normal workspace listing.
Record missing/unsupported objects explicitly and confirm no data reads
occur while sampling is off.

## R03 — Test PostgreSQL and leader failover

SQLite tests do not prove advisory locks, JSON persistence or lease
behavior on PostgreSQL. Exercise two control-plane replicas, a scan
lease takeover, heartbeat failure during a long batch, cancellation and
recovery. Fence stale scan writers so an expired holder cannot finish a
sweep after another holder has taken over. Verify event idempotence and
consistent asset identities across retries.

## R04 — Validate real runtime lineage and its semantics

Exercise actual Spark/Delta and DuckDB jobs, failed writes, transactions
rolled back after successful statements, temp views, aliases, streaming
and long-running kernels. Confirm the 200-event cap and de-duplication
report honest coverage gaps. Distinguish plan construction from
materialized reads and committed writes. Treat runner-reported lineage
as scoped evidence, not an independent security attestation.

## R05 — Keep a representative lineage corpus

Maintain versioned SQL, PySpark, DAX, M, TMDL and PBIR examples from
supported systems with expected nodes, edges, confidence and gaps.
Include dynamic names, unsupported constructs, renamed objects, wildcard
schema changes and malformed definitions. Compare impact-analysis
outputs before and after parser/dependency updates; measure false
positives and false negatives rather than only edge counts.

## R06 — Test governance at scale and across visibility changes

Load realistic large graphs and measure search, traversal, inherited
classification, policy evaluation and sampling latency. Require
truncation indicators where node/result caps apply. Test grant/revoke,
workspace remapping, merges, removed assets and hidden intermediate
nodes after caches are populated. Verify approved classifications remain
attached to the correct physical asset after identity repairs.

## R07 — Retention and privacy review

Review stored definition snippets, scanner imports, errors, statistics,
audit records, graph history and backups. Aggregates and numeric/date
min/max can still be sensitive. Enforce approved classifications
consistently, document retention/deletion behavior and confirm that a
rejected scan document or connector exception cannot echo credentials.
Keep exported review/debug bundles free of secrets and data values.

## R08 — Establish reproducible release inputs

The build uses broad lower dependency bounds. Preserve the exact
resolved dependency sets and SBOMs for each supported platform/Python
version and audit vendored UI assets, containers and runtime JARs
separately. Pin GitHub Actions to reviewed commit SHAs and test the
actual reusable workflows, signed artifacts and trusted publishers.
Advisory success on three Python 3.13 resolutions does not prove all
allowed dependency versions are compatible.

## R09 — Verify install, restore and upgrade paths

Test fresh installs and upgrades with an existing graph,
classifications, policies and histories. Back up and restore the full
database/secrets/runtime trust state; test schema migrations on both
databases and rollback/recovery instructions. Verify workers reconnect
without duplicated jobs, stale sessions or lost lineage. The local
wheel/init smoke tests are narrower than this acceptance matrix.

## R10 — Keep release claims aligned with evidence

Publish the first release with its explicit supported deployment shapes
and known limits. The supplied vision is broader than what public Fabric
APIs guarantee. Do not promise universal visual-to-physical lineage,
enforcement, streaming coverage or complete tenant discovery. Keep the
lineage support matrix, release notes, screenshots and CLI/API
references synchronized with the corrected implementation.

# Feature coverage against the new functionality

| **Area**              | **Implemented direction**                                                                                   | **What still needs proof or correction**                                                                                                                         |
|-----------------------|-------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Discovery             | Local catalog/storage, Fabric connector and scanner imports; scheduled/incremental scans.                   | Real-tenant behavior, completeness, retry state and API watermark/paging fixes.                                                                                  |
| Stable identity       | Asset registry, aliases, placeholders, merges and version history.                                          | Authoritative-ID collision handling, scoped name ambiguity, merge transaction/audit repair.                                                                      |
| Static lineage        | SQL, Python/PySpark, notebooks, pipelines, semantic models, DAX/M and report analyzers.                     | Context invalidation, parse-failure retention, unsupported construct gaps and representative corpus checks.                                                      |
| Runtime lineage       | Best-effort dataset events, durable inbox, observed evidence and retention.                                 | Resource isolation, actual Spark/Delta deployment coverage, event-loss visibility and semantics of lazy/transactional execution.                                 |
| Governance            | Glossary, ownership, domains/products, classifications, certification, approvals and observe-mode policies. | Sampling/profiling policy consistency; graph identity/completeness defects affect governance conclusions.                                                        |
| UI, CLI, SDK and MCP  | Rich intelligence pages and APIs; read-scoped MCP tools.                                                    | Browser tests pass with review environment corrections; CI must enforce that execution. Automated API tests do not replace live cloud or large-graph acceptance. |
| Enterprise operations | Health checks, audit, migrations, release builds and security workflows.                                    | Audit deadlock, scanner fixtures, multi-replica/failure tests, clean-host installs and actual publication workflow verification.                                 |

# Reproduction and evidence appendix

All code finding paths are linked to the reviewed source tree.
Reproductions live outside that tree and use temporary databases and
synthetic values; they do not contact a real tenant. The following files
preserve the exact observed inputs/results for follow-up engineering
work.

[probe.py](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/probe.py)
— Sampling policy inheritance, missing schema projection, automatic
profiling, SQLite blocking, page cap and modifiedSince selection.

[probe_more.py](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/probe_more.py)
— Mapped Fabric workspace identity collision, real SQLite merge audit
timeout and partial-checkpoint retry loss.

[probe_cache.py](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/probe_cache.py)
— Changed default lakehouse with unchanged etag, and parse-error scan
success with retired edges.

[probe_transport.py](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/probe_transport.py)
— Streaming fake response showing the byte cap is enforced after
buffering.

[evidence.json](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/evidence.json)
— Final consolidated validation counts, failure disposition, skipped
coverage and source integrity result.

[source-hashes.json](file:///Users/aimac/Documents/Codex/2026-10-08/users-aimac-desktop-projects/work/intelligence-review/source-hashes.json)
— SHA-256 manifest of the reviewed source snapshot, excluding
caches/build outputs and .DS_Store.

To rerun a probe on this machine, use the review virtual environment’s
Python followed by the linked script. The scripts contain the reviewed
build’s absolute path; update that path when moving them to another
checkout. Local regression artifacts are diagnostic evidence, not a
distributable release package.

**Automated selection** python -m pytest -q -m "not spark and not e2e
and not live and not postgres". The completed broad result is assembled
from four whole-module shards plus the separate intelligence suite.
Browser verification: tests/e2e/test_16_intelligence.py with
OLCF_E2E_CHROMIUM resolved by Playwright and
OLCF_COMPUTE\_\_SPARK_MASTER_PORT_SPAN=1 /
OLCF_COMPUTE\_\_SPARK_MASTER_UI_PORT_SPAN=1.

**Build and checks** bash scripts/build_release.sh --out
\<empty-review-output\>; ruff check .; mypy src/olcf/cli src/olcf/sdk
--ignore-missing-imports --follow-imports=silent; python
scripts/check_docs.py; explicit workflow paths to actionlint; gitleaks
dir . with repository configuration. Review logs record the full
executed commands and results.

## External references

[Microsoft Learn — Get Modified
Workspaces](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-modified-workspaces)
— Documents the timestamp age window and endpoint request limit
supporting F07.

[Microsoft Learn — Lakehouse List
Tables](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables)
— Documents continuation tokens and table response fields; used to check
the discovery contract.

[Microsoft Learn — Get Notebook
Definition](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/get-notebook-definition)
— Official definition retrieval contract referenced by the proposed
feature.

[Microsoft Learn — Power BI lineage in
Purview](https://learn.microsoft.com/en-us/purview/data-map-lineage-power-bi)
— Context for the supplied discovery/lineage vision; not evidence that
this implementation matches Purview.

[Playwright — Browser
binaries](https://playwright.dev/python/docs/browsers#managing-browser-binaries)
— Documents version-specific browser installation and cache resolution
supporting F11.

Official references checked 10 October 2026. Live-service contracts and
permissions should be rechecked against the release candidate and target
tenant.
