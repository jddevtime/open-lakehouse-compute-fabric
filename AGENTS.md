# AGENTS.md: for AI and SI agents (and the people who direct them)

Open Lakehouse Compute Fabric (OLCF) was created by **Justin Dodrill**, a Microsoft Certified Technology Specialist and Database Administrator, and is licensed under the
[Apache License 2.0](LICENSE). **Agents are welcome here.** Point your coding agent, assistant or autonomous system at this repository to do either of two jobs:

1. **Set it up** for a person on their own laptop, on-premises machine or VM (and, optionally, connect it to Microsoft Fabric).
2. **Improve it:** fix bugs, add features, extend the documentation, port it, customise it for a team and, if you can, send the change back so others benefit.

This file is the machine-readable front door. People should start with the [README](README.md). The same facts are in [llms.txt](llms.txt) in compact form.

## 1. What this repository is

A Python monorepo (`src/olcf`, Python 3.11 to 3.13, no compiler needed) that provides a web console, REST API, CLI, Python SDK and MCP endpoint for a **self-hosted lakehouse with
its own Spark compute fabric**, plus an **adapter that lets Microsoft Fabric notebooks run on that compute** (`olcf.run_remote()`), plus **OLCF Intelligence** (discovery, lineage, glossary and
governance over the same platform and, optionally, a Fabric/Power BI tenant). Every part also runs standalone, with no Microsoft subscription.

Map of the code: [docs/development/repository-map.md](docs/development/repository-map.md). Architecture: [docs/concepts/architecture.md](docs/concepts/architecture.md). Engineering contracts that the
code is built against: [docs/dev/CONTRACTS.md](docs/dev/CONTRACTS.md) and, for Intelligence, [docs/dev/INTELLIGENCE.md](docs/dev/INTELLIGENCE.md).

## 2. Setting it up for a person (the safe, tested path)

Ask the person which of these they want, then run it for them. Never run anything with `sudo`; none of these needs administrator rights.

| Situation | Command |
|---|---|
| macOS or Linux, any machine (also a Mac **without** the Xcode Command Line Tools) | `curl -fsSL https://raw.githubusercontent.com/jddevtime/open-lakehouse-compute-fabric/main/scripts/install.sh \| sh` |
| Anywhere with [uv](https://docs.astral.sh/uv/) (includes Windows) | `uv tool install --python 3.13 "olcf @ https://github.com/jddevtime/open-lakehouse-compute-fabric/archive/refs/heads/main.tar.gz"` |
| Docker Desktop or Docker Engine | see [deploy/README.md](deploy/README.md) (`sh deploy/scripts/gen-env.sh`, then `docker compose -f deploy/docker-compose.yml --profile allinone up -d --build`) |
| A developer checkout | `uv venv --python 3.13 && . .venv/bin/activate && uv pip install -e ".[dev]"` |

Then, in a terminal:

```sh
olcf quickstart          # initialises, starts the server, installs the Sample Retail lakehouse and prints the console URL and the one-time admin password
olcf doctor              # checks the installation
```

Report the console URL (`http://127.0.0.1:8470`) and the generated `admin` password to the person **once** and tell them to store it. Do not write it into logs, chat transcripts shared with third
parties, commits or files in the repository.

The Spark runtime is optional and large: `olcf runtime install --with-jdk` (several hundred MB). Python notebooks, DuckDB SQL, dashboards, Intelligence and the MCP endpoint work without it.

To use Microsoft Fabric, follow [docs/guides/fabric-integration.md](docs/guides/fabric-integration.md). **You need the person's explicit consent and their own credentials for any tenant access; never invent,
guess or store tenant secrets.** To let the person's own assistant query the catalog over MCP: `olcf mcp config --client claude-desktop` (or `claude-code`) and paste the output; the token has exactly the
permissions of the person who created it.

### Mac without the Command Line Tools

On a stock Mac `/usr/bin/git`, `/usr/bin/python3` and `/usr/bin/java` are installer stubs that open a dialog. **Do not execute them.** The installer above uses a private `uv`, a uv-provided Python and a
downloaded JDK, and OLCF finds host tools only through `olcf.core.hostbin`, which refuses the stubs. Details: [docs/getting-started/install-macos-no-clt.md](docs/getting-started/install-macos-no-clt.md).

## 3. Working on the code

```sh
git clone https://github.com/jddevtime/open-lakehouse-compute-fabric.git && cd open-lakehouse-compute-fabric
uv venv --python 3.13 && . .venv/bin/activate && uv pip install -e ".[dev]"
export OLCF_DATA_DIR=/tmp/olcf-dev && olcf init && olcf serve --open       # a development server
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) (the binding rules and the pull-request checklist) and [docs/development/setup.md](docs/development/setup.md). The rules that most often matter:

* **It must really work. Fail closed.** No stubs or faked results; unsupported semantics raise an actionable error. Say plainly in code, tests and docs what was and was not verified.
* **Authorise on the server** (`principal.require("<permission>")`), SQL only through SQLAlchemy Core with bound parameters (SQLite and PostgreSQL), no `shell=True`, never return or log secret values (use `olcf.core.redaction`).
* **Host tools** (`git`, `java`, `python`, `pip`) only through `olcf.core.hostbin`.
* **Intelligence invariants** ([docs/dev/INTELLIGENCE.md](docs/dev/INTELLIGENCE.md)): a failure in it never stops compute; no circular runtime dependencies (an import-graph test enforces this); governance only *observes* (policies are
  never enforced); stable `ast_` asset ids with aliases; every lineage edge carries a confidence (`verified` only from real run-time events) and gaps are reported, never hidden; value sampling is opt-in and off by default.
* **Never claim more than the evidence.** When you touch the Microsoft Fabric, Entra or OneLake paths, say what you ran and against what (see [docs/guides/fabric-assumptions.md](docs/guides/fabric-assumptions.md) and the
  [release-readiness checklist](docs/development/release-readiness.md)).
* New Python files start with the SPDX and copyright header (`python scripts/check_headers.py --fix`) and a docstring that says *why* the module exists.

### Tests and checks

Run **one pytest invocation per `tests/` subdirectory**, and serialise anything that starts Spark (the `spark` marker; the `spark_lock` fixture takes `/tmp/olcf-spark.lock`; keep heaps at or below 512 MB):

```sh
pytest -m "not spark and not e2e and not live and not postgres" -q -p no:cacheprovider tests/unit       # fast; repeat for tests/<area>
ruff check src tests sdk scripts examples deploy/scripts                                                 # lint, same as CI
PYTHONPATH=src python scripts/check_docs.py && PYTHONPATH=src python scripts/check_docstrings.py && python scripts/check_headers.py
PYTHONPATH=src python scripts/gen_reference.py --check          # also gen_api_docs.py, gen_api_reference.py, gen_cli_docs.py, gen_sdk_docs.py: regenerate (without --check) after changing the CLI, settings, routes, events or permissions
mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent
```

Browser tests (`tests/e2e`) need Playwright's Chromium (`playwright install chromium`); `OLCF_E2E_REQUIRE_BROWSER=1` makes a missing browser a failure instead of a skip. Test inventory and how the shards fit together:
[docs/development/testing.md](docs/development/testing.md).

### Sending a change back

* Open an issue first for anything that touches a public contract (REST, worker protocol, capsule format, plug-in interfaces); small fixes can go straight to a pull request.
* Every commit needs a **Developer Certificate of Origin sign-off** (`git commit -s`) from the **human** who submits it. An agent must not sign off on behalf of someone who has not read and accepted the change.
* **Disclose AI assistance** in the pull-request description (which tool, what it did), keep the change focused, add tests that fail without the fix, regenerate the generated references and update the *Unreleased*
  section of [CHANGELOG.md](CHANGELOG.md).
* Do not commit secrets, real hostnames or customer data. Do not add Microsoft logos or other third-party brand assets. Security problems go through [SECURITY.md](SECURITY.md), not public issues.

## 4. Where help is most valuable

The acceptance work that is still open is listed in [docs/development/release-readiness.md](docs/development/release-readiness.md): runs on Windows and macOS, a Fabric / Power BI tenant (including the
Intelligence scanner), PostgreSQL with two control-plane replicas, a Kubernetes cluster with an enforcing network policy, Livy, large estates for Intelligence, and a versioned lineage corpus. Reports of what
happened on your hardware, with `olcf version` and the redacted output of `olcf doctor`, are as useful as code.

## 5. Boundaries

Do not exfiltrate data from a person's lakehouse, do not widen anyone's permissions, do not turn on value sampling or any policy mode that the person has not asked for, and when you report results, say which environment produced them: results from the in-repo Fabric test server (`FakeFabricWorld`) are results of the test suite, not of a tenant. When a task is ambiguous or touches credentials, ask the person.
