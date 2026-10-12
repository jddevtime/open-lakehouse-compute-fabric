# Testing

The suite is pytest, with one directory per area. Most tests run in-process against a real application object (the real FastAPI app, a real SQLite database, real services) and touch the network only on loopback. A smaller set starts real processes: a server, worker agents, a Spark runtime, a browser.

## Layout

The counts are the number of tests pytest collects in each directory at the time of writing (6,236 in total, counted on 11 October 2026); they change with every pull request, so treat them as orders of magnitude.

| Directory | Tests | Covers |
|---|---|---|
| `tests/unit/` | 88 | foundation: configuration layering, a login/workspace/item smoke test, host-tool discovery, the rate limiter, response headers, the OpenAPI document's consistency, the cross-platform file lock (`test_filelock.py`), a simulated Windows start-up (`test_windows_startup.py`) and the layout rules of the test tree (`test_test_layout.py`) |
| `tests/intelligence/` | 1,903 | OLCF Intelligence. Top level (390): the asset graph (identity and merge, graph invariants: idempotent apply, evidence, sweep, diff), the engine and its analysis cache, every REST route (`test_api.py`, `test_api_write.py`), visibility and the role matrix (`test_visibility.py`), the event inbox and health, the run-time translation (`test_runtime_facts.py`), the MCP tools (`test_mcp_intel.py`, `test_mcp_gov.py`), failure isolation and the import-graph rule (`test_isolation.py`), and the regressions of the external code review: identity separation of remote workspaces mapped to one local workspace (`test_review_f04.py`), the merge audit record written on the merge's own connection (`test_review_f05.py`), the analysis-cache context fingerprint and incomplete analyses that keep the last known lineage (`test_review_f09_f10.py`), the graph writer pacing, the bounded event-bus persist and chunked maintenance transactions (`test_review_r01.py`). `lineage/` (812): one file per analyzer (SQL, PySpark, notebooks, OLCF and Fabric pipelines, Spark job definitions, OLCF reports, TMDL/model.bim, DAX, Power Query M, Power BI reports, scanner results) plus robustness and redaction suites that feed hostile and oversized input. `discovery/` (395): the local, catalog-mirror and Fabric connectors (the Fabric ones against `FakeFabricWorld`, a local test server), the scan runner, sampling and masking, the sensitive-data detector, the scanner import, source management. `governance/` (166): glossary, domains and products, ownership, classification and propagation, certification and change requests, policies, quality, authorization, isolation. `system/` (140): the sample workspace end to end (`test_sample_integration.py`, `test_full_platform.py`), deployment shapes, failure isolation including the event bus lanes, run-time lineage by a real worker, the Fabric-notebook-to-tenant link (`test_fabric_runtime_link.py`), authorisation across every surface a scale test of 2,000 tables and 20,000 columns and the write-lock isolation test (event publication and core writes stay within stated bounds while a 3,400-asset scan runs on SQLite). The `postgres`-marked variants run the graph tests on PostgreSQL when `OLCF_TEST_POSTGRES_URL` is set |
| `tests/security/` | 388 | authentication, tokens, RBAC and the permission matrix, OIDC/Entra/LDAP, secrets, audit chain, PKI, SSRF, redaction |
| `tests/data/` | 955 | storage providers, lakehouses, Delta/Iceberg/Parquet, the classic catalog and its lineage, SQL guard and engines, reports, export, MCP |
| `tests/items/` | 184 | notebooks, environments, variable sets, formats (ipynb, py, Fabric Git), Git sync, promotion |
| `tests/compute/` | 523 | scheduler and planner, job and session services, runtimes, providers, Spark clusters, worker hub and protocol |
| `tests/node/` | 389 | the worker agent: protocol, job supervision, kernels, daemons, runtime manager, manifest signing, Maven, layers |
| `tests/orch/` | 402 | pipelines: expressions, engine, triggers, webhooks, activities, Fabric import, observability, alerts |
| `tests/fabric/` | 311 | the Fabric adapter: capsule, signing, analyzer, shim, Entra, REST pull mode, SDK, diagnostics |
| `tests/ops/` | 134 | backup and restore, updates, retention, high availability, diagnostics, support bundle |
| `tests/tooling/` | 784 | CLI (including `olcf intelligence` and `olcf governance`: `test_cli_intelligence.py`, `test_cli_governance.py`), SDK (`test_sdk_intelligence.py`, `test_sdk_governance.py`), installers, release verification, documentation scripts |
| `tests/corpus/` | 65 | the analyzer against a corpus of Fabric notebooks with expected classifications |
| `tests/integration/` | 16 | the whole product: one server and two real agents, the sample workspace, a pipeline, MCP, Spark |
| `tests/web/` | 7 | the web client's pure logic, run with Node's test runner (needs Node, `OLCF_REQUIRE_NODE=1` makes a missing Node an error): the remembered sign-in (`remember_auth.test.mjs`, run by `test_remember_auth.py`) and the Intelligence pages' graph code (`intel_ui.test.mjs`, run by `test_intel_ui.py`: deterministic lineage layout, path and impact walks, SVG export, keyboard neighbours, search) plus static checks that every Intelligence route, navigation entry and help text is registered |
| `tests/e2e/` | 69 | the web console in headless Chromium against a real stack (16 files, numbered in the order of the story they tell; `test_14_first_run.py` signs in to a no-Spark installation and checks the Runtimes and Compute pages, the dashboard and the waiting banner of a queued Spark job; `test_16_intelligence.py` drives the Intelligence pages with the sample workspace and completed local and catalog scans: overview, catalog search to asset to lineage graph, column lineage, impact, test connectivity, gaps, coverage, scanner import, sampling policies and governance) |

Shared fixtures are in `tests/conftest.py`: `settings` (a fresh data directory), `ctx` (an application context with the foundation services), `make_app` and `client` (a `TestClient` with an admin token in `client.admin`), and `spark_lock`. Every directory under `tests/` is a package and `conftest.py` files hold **fixtures only**: shared helpers live in qualified modules that tests import by their full name (`tests/compute/ctrl_stack.py`, `tests/orch/orch_support.py`, `tests/items/items_support.py`, `tests/fabric/fabric_support.py`, `tests/spark_locking.py`), never as a bare `from conftest import ...` (`tests/unit/test_test_layout.py` fails the build otherwise). Doubles for the worker side live in `tests/node/fakes.py` and `fake_controller.py`; `tests/compute/fake_kube.py` (`FakeKube`) is a fake Kubernetes API server **and** kubelet (it mounts Secrets read-only whatever the mount says, runs the real runner and turns its output into timestamped pod logs); the Livy tests use `httpx.MockTransport` and a fake Livy.

## Running tests

```sh
pytest -q tests/unit                         # seconds
pytest -q tests/security tests/data          # the areas you touched
pytest -q -k "scheduler and not spark"       # by name
```

One invocation of the whole tree (`pytest tests`) collects without error (6,105 tests).

To prove that no test depends on collection order, set `OLCF_TEST_ORDER=reverse` or `OLCF_TEST_ORDER=shuffle:<seed>` (whole modules are reordered; the CI collection step uses `reverse`).

The default selection, which is what the CI `tests` job runs, leaves out everything that needs something external:

```sh
pytest -m "not spark and not e2e and not live and not postgres" -q -p no:cacheprovider --timeout 300 tests
```

`pyproject.toml` sets `asyncio_mode = "auto"` and a per-test `timeout` of 300 seconds (the `pytest-timeout` plug-in).

## Markers and what they need

| Marker | Meaning | Needs | CI job |
|---|---|---|---|
| `spark` | starts a JVM; serialised | a runtime (see below), Java | `spark` |
| `e2e` | browser tests (added automatically to everything under `tests/e2e`) | Playwright and its Chromium, plus a runtime for the Spark ones | `ui-e2e` |
| `live` | talks to real Fabric, OneLake or ADLS | the `OLCF_LIVE_*` variables; skipped unless you pass `-m live` | none |
| `postgres` | applied to the PostgreSQL tests of `tests/ops/test_ha.py`, `test_backup_pg.py`, `tests/security/test_keyring_concurrency.py` and to the `postgres` parameter of the graph fixture in `tests/intelligence/conftest.py`; all skip themselves when `OLCF_TEST_POSTGRES_URL` is unset | a PostgreSQL server | `postgres` |

At the time of writing pytest selects 39 tests with `-m spark`, 8 with `-m live`, 68 with `-m postgres` (gated on `OLCF_TEST_POSTGRES_URL`; they include the key-ring concurrency tests and the PostgreSQL variants of the Intelligence graph tests) and 69 with `-m e2e`. Of the 68, 53 are the PostgreSQL variants of the Intelligence graph tests; they run when `OLCF_TEST_POSTGRES_URL` points at a PostgreSQL server, and the default run covers SQLite.

Tests that need an optional package skip when it is missing (`pytest.importorskip`): `psycopg`, `boto3` and `moto` (S3 backup targets), `paramiko` (SFTP backup target), `pyiceberg`, `jsonschema`. `pip install -e ".[dev]"` installs all of them. Installer tests need `uv`.

### Spark tests

Spark tests use a real runtime, not a mock. Build the small pinned test runtime once; it is cached and reused:

```sh
export OLCF_TEST_RUNTIME_DIR=/tmp/olcf-test-runtime     # the default
python -m tests.node.runtime_fixture                    # builds it (needs Internet the first time)
pytest -m spark -q --timeout 900 tests/node tests/compute tests/fabric
```

Only one JVM test at a time may run on a machine: tests take an exclusive `flock` on `/tmp/olcf-spark.lock` through the `spark_lock` fixture (the browser suite uses the same file). The test runtime has Spark but not the Delta jars (they come from Maven Central); Spark with Delta and Iceberg is checked by `scripts/verify_spark_delta.py`, which the CI job `delta` runs on pushes.

### PostgreSQL tests

```sh
createdb -E UTF8 -T template0 olcf_test
export OLCF_TEST_POSTGRES_URL='postgresql+psycopg://user:pw@localhost/olcf_test'
pytest -q tests/ops/test_ha.py tests/ops/test_backup_pg.py tests/security/test_keyring_concurrency.py
pytest -q -m postgres tests/intelligence                        # the Intelligence graph tests on PostgreSQL
```

The high-availability tests start several control-plane replicas against one database and check leader election, failover and idempotent job submission. The CI `postgres` job selects them with `pytest -m postgres`.

### Browser tests

```sh
pip install -e ".[dev]" && playwright install --with-deps chromium
pytest -m "e2e and not spark" -q tests/e2e                 # without the JVM tests
OLCF_E2E_CHROMIUM=/path/to/chrome pytest tests/e2e         # use an existing Chromium instead of Playwright's own
OLCF_E2E_REQUIRE_BROWSER=1 pytest tests/e2e                # a missing browser is a failure, not a skip (CI sets this)
```

The browser is resolved by Playwright itself (the Chromium that `playwright install` fetched for the installed Playwright version, in `PLAYWRIGHT_BROWSERS_PATH` or the platform cache); `OLCF_E2E_CHROMIUM` is only an explicit override. Without a browser the tests skip on a developer machine and fail when `OLCF_E2E_REQUIRE_BROWSER=1`. The CI `ui-e2e` job installs Chromium and the cached runtime, writes `e2e-junit.xml` and runs `scripts/check_junit_run.py e2e-junit.xml --min-tests 60`, which fails the job when fewer than 60 tests executed or any test was skipped (pytest itself exits 0 when everything is skipped). A failed test leaves a full-page screenshot per page and, with `OLCF_E2E_TRACE=1` (set in CI), a Playwright trace under `test-results/` (`OLCF_E2E_ARTIFACTS` moves it); CI uploads that directory.

`tests/e2e/stack.py` starts a control plane with three real worker agents and the cached runtime. It allocates two disjoint blocks of free ports for the Spark master RPC and web UI (the production settings validator rejects overlapping ranges) and, when the control plane or an agent dies during startup, fails at once with the tail of its log instead of waiting for a timeout. Every test page records console errors, uncaught exceptions, failed requests and HTTP responses with status 400 or above, and the test fails if any were recorded unless it declared them with `page.expect_problems(...)`. For fast iteration, start one stack (`python -m tests.e2e.stack`), then run the tests against it with `OLCF_E2E_URL=http://127.0.0.1:8470 OLCF_E2E_DATA=<its data dir>`.

The cached test runtime (`tests/node/runtime_fixture.py`) holds only pyspark, pandas, pyarrow and the kernel packages: no `duckdb`, `deltalake` or `matplotlib`. Do not add them to its venv (for example with a `.pth` file pointing at a scratch `pip --target` directory, which silently vanishes when `/tmp` is cleaned): the Python-engine sample notebook `02_Retail_Explore_Python` asks for the `host-python` runtime family in its settings, so the e2e stack runs it on the workers' own interpreter (`olcf-host-python`, the environment of the test run, which has these libraries), exactly as a production installation does even when a Spark runtime is installed. `tests/compute/test_first_run_runtimes.py` and `tests/data/test_sample.py` pin that rule.

The suite writes screenshots to `docs/images/ui/` (those images are curated by the UI owners). Set `OLCF_E2E_SHOTS=<dir>` to write them elsewhere.

### Live tests

They are never run by default. To run them against your own tenant:

| Variable | Used by |
|---|---|
| `OLCF_LIVE_FABRIC_TENANT`, `OLCF_LIVE_FABRIC_CLIENT_ID`, `OLCF_LIVE_FABRIC_CLIENT_SECRET`, `OLCF_LIVE_FABRIC_WORKSPACE` | the token broker and workspace listing (all four required) |
| `OLCF_LIVE_FABRIC_NOTEBOOK` | fetching a notebook definition (skipped without it) |
| `OLCF_LIVE_ONELAKE_*`, `OLCF_LIVE_ADLS_*` | storage read/write against OneLake or ADLS Gen2 (see the variable names read at the top of the test modules that carry the `live` marker) |

```sh
pytest -m live -q tests/fabric/test_live.py
```

## How tests are written here

* **Fail without the change.** A test for a bug must fail on the old code. Say in the pull request how you checked.
* **Authorisation is tested from the outside.** Use the API with tokens of different roles, not calls into the service. `tests/security/test_authorization.py` builds two tenants and attacks one tenant's object ids with the other tenant's roles on every route, then checks role separation, read-only principals, token scopes and role escalation.
* **Both databases.** Anything that touches SQL must be valid on SQLite and PostgreSQL. If a behaviour differs by dialect, branch on `db.dialect` and add a PostgreSQL test.
* **Real over mocked, doubles at the edge.** Prefer the real service on a temporary data directory. Use the fakes in `tests/node/fakes.py` for the worker side and a fake HTTP server for Entra and Fabric; say in the test's docstring what the double does not prove.
* **No test may depend on another** (except the ordered stories in `tests/integration` and `tests/e2e`, which say so), the network (other than loopback), the clock (inject it), or a fixed port (ask the OS for a free one).
* **Clean up processes.** Start subprocesses through the fixtures, which terminate them; a test that leaks a JVM or a server will make the next one fail.
* **Secrets in fixtures** are obviously fake. Ruff's `S105`/`S106` rules would otherwise flag them; add a targeted `# noqa` with a reason.

## Known issues

The test setup problems found while writing the first version of this documentation are fixed: `pytest tests` collects in one invocation (the two `test_metrics.py` files no longer share a basename: the orchestration one is `tests/orch/test_obs_metrics.py`) and the `postgres` marker selects the PostgreSQL tests. What remains is listed in [requirements traceability](../compliance/requirements-traceability.md#known-limitations):

1. **The Docker, Helm, Kubernetes, Livy, Windows and macOS paths** are covered by rendered manifests, protocol tests with test doubles and Linux-hosted tests of the platform-specific code. In particular: `test_kubeconform` and the Helm test are skipped when `kubeconform` / `helm` are absent; the CI job `windows` and the macOS matrix legs are configured to run on GitHub's runners; the Kubernetes provider is exercised against `FakeKube`.
2. **The first-run path** (`olcf quickstart --no-runtime` in a subprocess) is tested on Linux, without the Spark runtime download.
