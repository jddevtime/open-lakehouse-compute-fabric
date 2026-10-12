# Development setup

This page gets you from a clone to a running server, a passing fast test run and a clean pre-pull-request check. The contribution rules are in [CONTRIBUTING.md](../../CONTRIBUTING.md); the layout of the code is in the [repository map](repository-map.md).

## What you need

| Item | Needed for | Notes |
|---|---|---|
| Python 3.11, 3.12 or 3.13 | everything | CI runs 3.11 and 3.13 on Linux and 3.13 on macOS. Develop on 3.13 and keep the code valid on 3.11 (for example, no backslashes inside f-string expressions) |
| [uv](https://docs.astral.sh/uv/) (recommended) or `pip` | the virtual environment | nothing is compiled; wheels exist for every dependency |
| A JDK 17 or 21 | tests marked `spark`, `olcf compute smoke` | `olcf runtime install --with-jdk` can download one |
| PostgreSQL | tests that need it, HA work | optional; CI uses 17, the HA tests were also run on 16; see below |
| Node.js | nothing | the web console has no build step |

## Clone and install

```sh
git clone https://github.com/jddevtime/open-lakehouse-compute-fabric.git && cd open-lakehouse-compute-fabric
uv venv --python 3.13 && . .venv/bin/activate && uv pip install -e ".[dev]"
#   or: python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
```

`[dev]` installs the test and lint tools and the optional extras (`postgres`, `s3`, `azure`, `k8s`, `keyring`, `iceberg`, `ldap`, `sftp`, `zstd`). The runtime dependencies are listed in `pyproject.toml`; every module that imports a third-party package at module level must have it there. Spark is **not** a dependency of the server: runtimes are separate virtual environments created under the data directory by `olcf runtime install`.

Because `olcf` is installed in editable mode, the `olcf` and `olcf-worker` commands exist in the environment. Without installing, use `PYTHONPATH=src python -m olcf.cli.main ...`; the scripts in `scripts/` expect `PYTHONPATH=src`.

## Run a development server

```sh
export OLCF_DATA_DIR=/tmp/olcf-dev          # keep dev state away from ~/.olcf
olcf init --port 8470                       # writes config.toml, the database, a CLI context; prints the admin password once
olcf serve --open                           # API, console, scheduler and an embedded worker
olcf sample install                         # the Sample Retail workspace
```

`olcf serve` runs in the foreground; `--daemon` detaches and `olcf stop` stops it. Useful switches: `--no-embedded-worker` (a control plane only, to test enrollment), `--tls`, `--workers N` (several API processes; needs PostgreSQL), `--log-level debug`, and the environment variable `OLCF_LOG_JSON=true`. Every setting is in the [configuration reference](../reference/configuration.md).

The REST contract is browsable at `http://127.0.0.1:8470/v1/docs` (Swagger UI) and `/v1/openapi.json`.

### The web console

`src/olcf/web/static` is plain JavaScript modules, Preact and htm, served as files. There is no bundler, no `node_modules` and no CDN at run time: edit a file and reload the browser. Third-party assets are vendored unmodified under `vendor/` with their licences and checksums (`vendor/VENDOR.md`); do not add a dependency without adding it there. The browser tests (`tests/e2e`, Playwright) are described in [testing](testing.md#browser-tests).

### More workers

```sh
olcf worker enroll-token --name dev-2                       # on the control plane
olcf-worker run --url http://127.0.0.1:8470 --token olcf_enr_... --name dev-2 --state-dir /tmp/olcf-dev-w2
```

Several agents on one machine are enough to exercise placement, drain and failure handling; they cannot tell you anything about real network behaviour.

### A Spark runtime

```sh
olcf runtime install --with-jdk && olcf compute smoke
```

downloads and verifies a runtime and runs a real Spark job. For tests, `python -m tests.node.runtime_fixture` builds a smaller pinned runtime once into `OLCF_TEST_RUNTIME_DIR` (default `/tmp/olcf-test-runtime`) and the Spark tests reuse it.

### PostgreSQL

SQLite is the default. To develop against PostgreSQL create an empty UTF-8 database (a database with `SQL_ASCII` encoding makes the driver return `bytes` and breaks SQLAlchemy), and set `OLCF_DB__URL`:

```sh
createdb -E UTF8 -T template0 olcf_dev
export OLCF_DB__URL='postgresql+psycopg://user:pw@localhost/olcf_dev'
```

The tests that need a server read `OLCF_TEST_POSTGRES_URL` and skip themselves when it is unset.

## Checks before a pull request

```sh
ruff check src tests sdk scripts examples deploy/scripts          # lint, same as CI
PYTHONPATH=src python scripts/check_headers.py                    # SPDX and copyright header on every Python file (--fix adds missing ones)
PYTHONPATH=src python scripts/check_docstrings.py                 # module, class and function docstring coverage
PYTHONPATH=src python scripts/check_docs.py                       # links, anchors, CLI commands and settings named in the docs, generated pages up to date
pytest -q tests/unit                                              # quick; then the areas you touched, see testing.md
```

`mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent` must pass; the rest of the package is type-checked informationally in CI.

## Generated documentation

These pages are produced from the code. Regenerate them after changing the CLI, settings, permissions, events, errors, health checks, MCP tools, API routes, the Python SDK, the Fabric SDK or the analyzer rules, and commit the result:

```sh
PYTHONPATH=src python scripts/gen_reference.py        # reference/{cli,configuration,permissions,health-checks,errors,events,mcp-tools}.md
PYTHONPATH=src python scripts/gen_api_docs.py         # reference/api.md and docs/api/openapi.json
PYTHONPATH=src python scripts/gen_sdk_docs.py         # reference/{sdk,fabric-sdk,fabric-rules}.md
```

Each takes `--check` (exit status 1 when a file would change), which is what `scripts/check_docs.py` and the CI docs job use. `scripts/gen_cli_docs.py` is a front end to the `cli` section of `gen_reference.py`.

## Layout of a change

A typical feature touches a package under `src/olcf/<area>/`, its tests under `tests/<area>/`, possibly a database migration stream (see the next section), the generated references, `CHANGELOG.md` (under *Unreleased*) and, when behaviour that users see changes, a guide. If the change alters a contract between packages, update `docs/dev/CONTRACTS.md` in the same pull request.

### Database changes

Tables are defined with SQLAlchemy Core in `olcf/<package>/schema.py` on the shared `metadata`, each package registers a migration *stream* named after itself, and every statement must run on SQLite and PostgreSQL (branch on `db.dialect` where you must). `olcf db status`, `olcf db migrate` and `olcf db rollback` operate on the streams. Never alter an applied migration; add a new one.
