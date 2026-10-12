# Getting started

This section takes you from nothing to a working OLCF with a sample lakehouse, a notebook and an AI agent connected. Pick an install route, then follow the quickstart.

| Page | What it covers |
|---|---|
| This page | requirements, the four install routes, quickstart, upgrade, uninstall |
| [macOS without the Xcode Command Line Tools](install-macos-no-clt.md) | how OLCF avoids the installer stubs, what to expect, troubleshooting |
| [First-run tour](first-run-tour.md) | a walk through the console, page by page |

## Requirements

OLCF is a Python application (Python 3.11, 3.12 or 3.13) with a browser console. What you need depends on how much of it you use.

| You want | You need |
|---|---|
| The console, REST API, lakehouses, SQL on DuckDB, dashboards, MCP, and Python notebooks (on the built-in host-Python runtime, no Spark) | Python 3.11+ (the installer provides one) and about 650 MB of disk for the Python packages (see below) |
| Charts in the sample Python notebook | `matplotlib`: the `plots` extra (`pip install "olcf[plots]"`, about 90 MB more). Without it the notebook still runs and shows tables |
| Spark jobs and notebooks on the Spark runtime | a runtime (`olcf runtime install`: a download from PyPI and Maven Central; the project's test runtime occupies 929 MB installed), a Java 17 or 21 JDK (downloaded for you with `--with-jdk` if the machine has none) |
| Several machines in one Spark cluster | an OLCF worker agent on each machine and network paths between them: [ports](../reference/ports-and-networking.md) |
| More than one control-plane process | PostgreSQL: [high availability](../guides/high-availability.md) |
| Git sync of workspaces | a real `git` on `PATH` (without one, Git endpoints answer `503 git_unavailable`; everything else works) |

The base install includes `ipykernel` (the kernel that runs notebooks) and `pandas` (the sample notebook and the host-Python runtime), so that Python notebooks work with no Spark download. Measured on Linux with CPython 3.13 in the build environment: the packages `olcf` needs before those two occupy roughly 460 MB installed, `ipykernel` with IPython, `debugpy` and the rest of its dependencies about 60 MB, and `pandas` with `numpy` about 135 MB; `matplotlib` and its dependencies add about 90 MB on top. Sizes on macOS, Windows and other Python versions were not measured. The Spark runtime is a separate, larger download (`olcf runtime install`).

The scheduler places a Spark job on a Spark Standalone cluster when some machine has at least 2 usable cores and 2048 MB of usable memory after its reservation (`compute.reserve_cores`, default 1 core, and `compute.reserve_memory_fraction`, default 15 %). On a smaller machine the job runs with the `local` provider instead and the placement note says why. Sizing guidance: [sizing](../operations/sizing.md).

Nothing in OLCF needs administrator rights, a compiler or the Xcode Command Line Tools to install. Supported operating systems are those the installers and CI cover: Linux and macOS (`scripts/install.sh`), Windows 10/11 (`scripts/install.ps1`), plus Docker on any of them. See [Status and maturity](../../README.md#status-and-maturity) for what the tests cover.

**Windows status.** Windows 10/11 is a supported target. Every inter-process lock goes through `olcf.core.filelock` (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows) and the POSIX-only names (`resource`, `os.getloadavg`, process groups and signals) are guarded. A targeted CI job (`windows`) covers the file lock, application start-up, Git sync and the dependency layers; the full test suite is a Linux job. Spark runtimes on Windows, process-group handling of jobs and signals differ from POSIX; run workers on Linux or in Docker for anything that matters.

## Install

All routes end with the same server (`olcf serve`) and CLI (`olcf`). Full details of every route, including the offline bundle and service units, are in [`deploy/README.md`](../../deploy/README.md).

### Route 1: installer script

```sh
# macOS and Linux: installs the newest source of the main branch (add --ref v0.1.0 for a tag)
curl -fsSL https://raw.githubusercontent.com/jddevtime/open-lakehouse-compute-fabric/main/scripts/install.sh | sh

# from a checkout or a release download:
sh scripts/install.sh --dry-run                                  # show what would happen
sh scripts/install.sh --wheel dist/olcf-0.1.0-py3-none-any.whl   # install a wheel
```

```powershell
# Windows PowerShell 5.1 or 7, per user. Install uv, then OLCF from the GitHub source archive (every dependency is a wheel; nothing is compiled).
# The PowerShell installer scripts\install.ps1 is for a downloaded wheel or an offline bundle.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
uv tool install --python 3.13 "olcf @ https://github.com/jddevtime/open-lakehouse-compute-fabric/archive/refs/heads/main.tar.gz"
olcf quickstart
```

Useful options (`sh scripts/install.sh --help` lists all): `--extras postgres,s3` (or `all`), `--no-runtime`, `--no-jdk`, `--prefix DIR`, `--data-dir DIR`, `--with-service`, `--offline-bundle DIR`, `--docker` (prints the Docker route and exits). The installer never runs `git`, `python3`, `java`, `clang` or `make`.

### Route 2: uv or pip

```sh
uv tool install "./dist/olcf-0.1.0-py3-none-any.whl[all]"   # olcf and olcf-worker on PATH
olcf init                                                    # data directory, keys, first administrator
olcf runtime install --with-jdk                              # optional: the Spark runtime
olcf serve --open
```

The wheel is built with `sh scripts/build_release.sh --out dist` ([release process](../development/release-process.md)); there is no public package index entry yet. Extras: `postgres`, `s3`, `azure`, `k8s`, `keyring`, `iceberg`, `ldap`, `sftp`, `zstd`, `plots` (matplotlib, for the charts of the sample Python notebook), `all` (includes `plots`). The installer script takes them with `--extras` (for example `--extras plots,postgres`); the container image defaults to `postgres,s3,k8s`, so add `plots` to `OLCF_EXTRAS` there if you want the charts.

### Route 3: Docker Compose

```sh
sh deploy/scripts/gen-env.sh
docker compose -f deploy/docker-compose.yml --profile allinone up -d --build
docker compose -f deploy/docker-compose.yml logs olcf | grep -i password
docker compose -f deploy/docker-compose.yml exec olcf olcf runtime install
```

Profiles: `allinone` (one container, SQLite, embedded worker, published on `127.0.0.1:8470`), `cluster` (PostgreSQL, MinIO, two workers) and `tls` (reverse proxy). Kubernetes: [operations/kubernetes](../operations/kubernetes.md).

### Route 4: from source

```sh
uv venv --python 3.13 && uv pip install -e ".[dev]"
olcf init && olcf serve
```

See [development setup](../development/setup.md).

## What `olcf init` creates

`olcf init` is idempotent. It creates the data directory (default `~/.olcf`, or `$OLCF_DATA_DIR`, or `--data-dir`), writes a commented `config.toml` (mode 0600), creates the database (SQLite unless you pass `--database-url`), generates the master key, the JWT signing key and the built-in certificate authority, creates the first administrator `admin` with a generated password, and stores a CLI context named `local`. The password is printed once and also saved to `secrets/bootstrap-admin.txt` (mode 0600); delete that file after you have stored the password.

The data directory contains `artifacts/`, `backups/`, `jobs/`, `logs/`, `pki/` (the CA), `runtimes/`, `secrets/`, `storage/` (local lakehouses), `workers/` and `tmp/`, plus `config.toml` and `olcf.db`. Back it up with `olcf backup`, not by copying files: [backup and restore](../guides/backup-and-restore.md).

## Quickstart (about five minutes)

```sh
olcf quickstart
```

One command: initialise, start the server in the background, install the sample workspace (`--no-sample` skips it), print a working state, then install the Spark runtime (skip with `--no-runtime`; `--no-jdk` never downloads a JDK) and open the console (`--no-open` skips that). The order is deliberate: the server and the sample come first and are reported before the runtime download starts, because that download (PySpark and jars from PyPI and Maven Central) is the step most likely to fail. If it fails (no route to Maven Central, a proxy, no network) quickstart still succeeds, prints a warning with a hint and the exact retry command (`olcf runtime install <id> [--with-jdk]`), and Python notebooks, SQL, dashboards and the AI-agent endpoint keep working: without a Spark runtime, Python-engine work runs on the worker's own Python (runtime `olcf-host-python`, shown by `olcf runtime list` as `no Spark`). Running `olcf quickstart` again is safe.

This is the output of `olcf quickstart --no-runtime --no-open --port 8530` on an empty data directory (the `data:` path is shortened; the password is generated, shown once and also saved in `<data dir>/secrets/bootstrap-admin.txt`; delete that file after storing it):

```text
==> checking this machine
==> initialising
==> starting the server
==> installing the sample workspace

OK: OLCF is running at http://127.0.0.1:8530
sample: Sample Retail (installed)
python: notebooks run on the built-in host-Python runtime (no Spark needed)
data:   /home/me/.olcf

Sign in to the web UI with (shown once - save it now):
  username: admin
  password: <generated-password>

Next steps:
  dashboard:   http://127.0.0.1:8530/#/dashboards/itm_01M4CRQK3MH7N7N7GWZFMG6EV1
  notebook:    olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python --wait
  AI agents:   olcf mcp config --client claude-code      (prints a ready-to-paste MCP client configuration)
  SQL:         olcf sql run -w "Sample Retail" 'select count(*) from retail_lakehouse.fact_sales'
  also:        olcf status      olcf stop

No Spark runtime was requested. Add one any time with: olcf runtime install   (needed only for Spark notebooks, Spark SQL and Spark jobs)
```

With the default port the URLs are `http://127.0.0.1:8470`; without `--no-runtime` the runtime install follows the block above. On a second run the sample reads `(unchanged)` and, instead of a password, the output says where the credentials file is. [How each step works](../guides/cli.md#what-olcf-quickstart-does-in-order).

Then:

```sh
olcf whoami
olcf sample status
olcf sql run -w "Sample Retail" "select count(*) from retail_lakehouse.fact_sales"
olcf notebook list -w "Sample Retail"
olcf intelligence summary                    # what the sample installed into the asset graph (needs the Intelligence module)
olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python --wait    # runs on the host-Python runtime: no Spark needed
olcf mcp config --client claude-desktop      # or --client claude-code
olcf status
olcf stop
```

The notebook run takes a few seconds and prints the job record (`runtime: olcf-host-python`, `state: succeeded`). The charts in it appear only when `matplotlib` is installed (`pip install "olcf[plots]"`); without it the notebook prints how to get them and shows tables instead. The other sample notebook, `01_Retail_Medallion_ETL`, uses Spark: until `olcf runtime install` has run, `olcf notebook run -w "Sample Retail" 01_Retail_Medallion_ETL` queues and `olcf job status <id>` prints `waiting: [no_runtime] ...` with the command that fixes it.

`olcf doctor` checks the machine and the installation and prints the command that fixes each problem. Its `runtime` line counts the built-in runtime: once the server has started on a fresh install it reads `OK runtime olcf-host-python verified in <state dir>`, and the server-side check reads `OK server: runtimes olcf-host-python: ready on 1/1 node(s); no Spark runtime is installed yet (optional: ...)`. Right after `olcf init`, before any worker has run, it still warns `WARN runtime no Spark runtime installed` with the fix `olcf runtime install --with-jdk`. `olcf runtime list` shows which Spark runtimes are installed (`not installed` otherwise).

Next: [first-run tour](first-run-tour.md), [add machines to the cluster](../guides/cluster-and-control-panel.md), [where does my data come from? (Intelligence)](../guides/intelligence.md#a-first-look-with-the-sample), [connect Microsoft Fabric](../guides/fabric-integration.md).

## Running as a service

```sh
olcf service install      # per-user: LaunchAgent (macOS), scheduled task (Windows), systemd --user (Linux)
olcf service status
olcf service uninstall    # keeps your data
```

`olcf service install --system --user NAME` installs a system-wide systemd unit on Linux. `olcf service print` shows the definition without installing it. Hardened unit files are in `deploy/systemd/`.

## Upgrade

```sh
olcf upgrade --version 0.2.0     # installs next to the current version, backs up and migrates the database, switches after a smoke test
olcf upgrade --rollback          # back to the previous version
```

`olcf update` takes the same options. A pip or uv install is upgraded with `uv pip install -U olcf && olcf db migrate`; a container with a new image tag or `helm upgrade`. Signed update manifests, preflight checks and maintenance windows are described in [operations](../operations/runbook.md#updates).

## Uninstall

```sh
sh scripts/uninstall.sh --dry-run          # lists everything it would remove
sh scripts/uninstall.sh                    # removes the program, keeps your data
sh scripts/uninstall.sh --remove-metadata --remove-logs      # data categories are opt-in
sh scripts/uninstall.sh --purge --yes      # everything, without asking
```

Categories: `--remove-metadata` (database, config, jobs, artifacts), `--remove-secrets` (master and signing keys, bootstrap password, built-in CA; irreversible), `--remove-logs`, `--remove-storage` (lakehouse files inside the data directory), `--remove-runtimes`, `--remove-backups`, `--remove-cli-config`. On Windows use `scripts\uninstall.ps1` with the PascalCase switches (`-RemoveMetadata`, `-RemoveSecrets`, `-Purge`, `-Yes`, `-DryRun`, ...).

## Troubleshooting

Start with `olcf doctor`, then see [troubleshooting](../guides/troubleshooting.md). On a Mac that cannot install the Command Line Tools: [install-macos-no-clt](install-macos-no-clt.md).
