# Response to the OLCF Intelligence code review of 10 October 2026

Author: Justin Dodrill. Written 11 October 2026, for [`CODE_REVIEW_2026-10-10.md`](CODE_REVIEW_2026-10-10.md) (the third external review, "Intelligence, discovery, governance and lineage"). The earlier reviews and answers are [`CODE_REVIEW_2026-10-08.md`](CODE_REVIEW_2026-10-08.md), [`CODE_REVIEW_RESPONSE.md`](CODE_REVIEW_RESPONSE.md), [`CODE_REVIEW_2026-10-09.md`](CODE_REVIEW_2026-10-09.md) and [`CODE_REVIEW_RESPONSE_2.md`](CODE_REVIEW_RESPONSE_2.md).

The reviewer's probe scripts were not available to us. For each finding we read the cited code, **reproduced the defect with a new test where we could**, fixed it, and kept the test. The recommendations (R01 to R10 of the review) are mostly live acceptance work; they are tracked as [R19 and R20 in release-readiness](../development/release-readiness.md) and **none of the live ones is marked done**.

Everything here was exercised on Linux with SQLite, test doubles (`FakeFabricWorld`, scripted transports) and a local Spark runtime.

## Scorecard

| ID | Priority | Disposition | What changed | Main tests |
|---|---|---|---|---|
| F01 Column sample bypasses the table's sampling ban | P1 | **Fixed** | The policy is resolved for the parent table; a column inherits it and can only narrow it. Denial happens before any reader is opened | `tests/intelligence/discovery/test_data_policy.py` (25 tests, a reader spy; 23 failed on the old code) |
| F02 Value profiling ignores asset policies and classifications | P1 | **Fixed** | Profiling and sampling share one `DataReadPolicy`. A table-level ban or `max_rows=0` means zero storage reads; approved classifications and sensitive names are excluded; a provider error fails closed; every authorised read is audited without values | same file |
| F03 A column sample expands to all columns without schema metadata | P2 | **Fixed** | The projection is applied on the declared-schema and reader-schema paths; an absent column is a 400, never the whole table | same file |
| F04 Mapped Fabric workspaces collapse distinct tables | P1 | **Fixed**, no automatic repair of old data | Authoritative aliases decide identity; qnames are hints. Conflicts are an informational `identity_conflict` gap and `identity_conflicts()` lists existing ones; the operator repairs with `move_alias` and a rescan | `tests/intelligence/test_review_f04.py` |
| F05 Merge audit deadlocks on SQLite | P1 | **Fixed** | `AuditLog.record(conn=)` writes in the merge's own transaction; notifications go through a post-commit outbox. One verifiable audit row per merge; merges finish in under 1 s | `test_review_f05.py` |
| F06 Partial Fabric scans advance the watermark past failed workspaces | P1 | **Fixed** | A durable per-workspace retry set, stored with the source checkpoint, is added to every incremental selection with back-off (5 min doubling to 6 h) | `test_fabric_incremental.py` |
| F07 Invalid `modifiedSince` | P2 | **Fixed** against the documentation, not against the service | A watermark kept 35 minutes behind, never older than 30 days minus 1 hour, a tenant-wide shared request budget (`intelligence.fabric_modified_requests_per_hour`, default 30), one retry on 400 | same file |
| F08 Pagination caps return incomplete inventories as complete | P2 | **Fixed** | `IncompleteInventory` is raised on a page cap or a repeated token; the connector records a blocking gap, the scan ends `partial` and no sweep runs (for the whole source, which is broader than the affected scope) | `tests/fabric/test_rest_bounds.py` (38 tests) |
| F09 Analysis cache ignores resolution context | P2 | **Fixed** | A context fingerprint is part of the cache key; migration v3 | `test_review_f09_f10.py` |
| F10 A parse-error scan succeeds and retires lineage | P1 | **Fixed** | `ApplyStats.blocking_gaps` and `incomplete`; last-known edges are kept and flagged stale, the scan is `partial`, nothing is swept; only a complete re-analysis retires | `test_review_f09_f10.py`, `test_scan_completeness.py` |
| F11 The browser CI job can pass while skipping | P1 | **Fixed in code** | Playwright's default resolution (`OLCF_E2E_CHROMIUM` overrides); `OLCF_E2E_REQUIRE_BROWSER=1` turns a missing browser into a failure; the job gates on a JUnit file (`scripts/check_junit_run.py`: a minimum run count, no skips) and uploads artifacts on failure | `tests/tooling/test_check_junit_run.py` (12) |
| F12 Overlapping Spark master port spans | P2 | **Fixed** | `allocate_master_ports()` gives two disjoint blocks that the production validator accepts; a dead start-up fails at once with the log tail | `tests/tooling/test_e2e_stack_ports.py` (307) |
| F13 Fixtures fail gitleaks | P2 | **Fixed** | The three values are built at run time; no allowlist or directory exclusion. gitleaks 8.24.2 (the workflow's pin, checksum verified) reproduced the three findings and now reports none | the scan itself |
| F14 OneLake byte cap checked after buffering | P2 | **Fixed** | Every body is a bounded stream: refused unread above the cap by header, counted regardless, stops at cap plus one chunk, decompression is bounded, the stream is closed on every path | `test_rest_bounds.py` |
| F15 Performance test breaks Windows collection and macOS memory units | P1 | **Fixed in code** | A lazy, platform-aware helper (`tests/peak_memory.py`) and a fresh subprocess for the measurement; the unit logic is tested for linux, darwin and win32 by monkeypatch | `tests/tooling/test_peak_memory.py` (13) |
| F16 Stale first-run fallback expectation | P2 | **Fixed** | The test asserts the explicit `host-python` selection and no fallback event; a separate test keeps the genuine fallback path | `tests/integration/test_first_run_no_runtime.py` |
| S3 diagnostic is path-dependent | P3 | **Fixed** | The probe step carries a structured `code` (`NoSuchBucket`) and the test asserts it; it passes under a 120-character base directory | `tests/ops/test_backup_targets.py` |

## Recommendations

| Review item | Disposition |
|---|---|
| R01 Failure isolation at the resource boundary | **Partly done.** On SQLite a 4,010-asset scan never held the writer lock for more than 51 ms, yet one event publish took 4.2 s (SQLite's retry sleeps plus a back-to-back writer). We added a write pacer (`intelligence.write_batch_max_rows` 500, `write_batch_max_ms` 250, `write_yield_ms` 100), bounded sweeps, retirement and pruning transactions, and an event-bus persist budget of 0.25 s with an ordered background persister. Measured with a 3,400-asset scan running: worst publish about 0.26 s, worst core write about 0.33 s, a scan about 40 % slower. The system test `test_write_isolation.py` states the bounds. **Not done:** PostgreSQL runs, pool exhaustion, memory and CPU budgets, a separate process. Documented as R19 |
| R02 Real-tenant acceptance | **Not done.** The acceptance matrix is written in `docs/guides/fabric-assumptions.md`; R15 and R20 track it |
| R03 PostgreSQL and failover | **Partly done.** The scan runner confirms and extends its lease before closing a scan, and a holder that lost the lease ends `failed` and sweeps nothing (a test on SQLite). No PostgreSQL run, no two replicas |
| R04 to R09 | **Not done**; recorded as R20 (a to h) |
| R10 Release claims aligned with evidence | The documentation was updated for each behaviour change; the open gates are in release-readiness. The README rewrite is still ahead |

## Things worth knowing

* **Merged-away assets and policies.** A sampling policy keyed to an asset id that was later merged away is followed only through `merged_target`; policies are not re-pointed on merge.
* **Not-scannable workspaces** (a scanner result that names a workspace and returns nothing for it) are not retried, because the condition is usually permanent.
* **Sweeps.** A partial scan suppresses removal sweeps for the whole source, not only the affected workspace. This is safer and broader than the review asked for.
* **The CI browser job** now builds or restores the Spark runtime so the notebook tests do not skip under the strict gate. That step is the most likely to need adjustment on the first real run.
* **Windows and macOS.** The memory helper and its unit conversion are tested by monkeypatch only.
* **UI.** The sidebar brand now reads "OLCF" over "Open Lakehouse Compute Fabric", and the notebook screenshot (executed cells, a chart and a table) is in the documentation images.
* **One flaky test** was seen once under load, `tests/security/test_tls.py::test_client_certificate_reaches_http_and_websocket_scope` (a websocket handshake timeout); it passes alone.

## Verification record

One pytest invocation per `tests/` directory on Linux, Python 3.13, SQLite, with the marker filter `not spark and not e2e and not live and not postgres` unless noted:

| Shard | Result |
|---|---|
| unit / web / integration | 87 passed, 1 skipped / 7 passed / 14 passed |
| data / orch / items | 953 / 402 / 184 passed |
| compute / node / fabric / corpus / ops | 520 / 371 / 343 / 54 / 124 passed |
| security | 383 passed, 2 skipped |
| tooling | 779 passed, 5 skipped |
| intelligence | 1,823 passed (80 PostgreSQL tests deselected) |
| `-m spark` (integration, compute, node, fabric, corpus) | 2, 4, 18, 1 and 10 passed (1 skipped) |
| e2e (headless Chromium, real stack, `OLCF_E2E_REQUIRE_BROWSER=1`) | 69 passed, 0 skipped (`scripts/check_junit_run.py` confirms) |

Also clean: ruff, the header, docstring and docs checks, every `gen_*.py --check`, mypy on `cli` and `sdk` (the CI invocation), and gitleaks 8.24.2 on the tree.

Not run: PostgreSQL, a real Fabric or Power BI tenant, Windows, macOS, GitHub Actions, Docker, Helm.
