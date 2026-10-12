# Contributing to OLCF

Thank you for considering a contribution. OLCF is created and maintained by Justin Dodrill and is licensed under the
[Apache License 2.0](LICENSE). By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Security problems are **not** reported as issues: follow [SECURITY.md](SECURITY.md).

## Ways to help

* Report a bug or a documentation error (use the issue forms; include `olcf version` and, where useful, the redacted output of `olcf doctor`).
* Propose a feature by opening an issue first, especially anything that touches the public contracts in [`docs/dev/CONTRACTS.md`](docs/dev/CONTRACTS.md)
  (REST API, worker protocol, capsule format, plug-in interfaces). Small fixes can go straight to a pull request.
* Add a connector, activity, storage provider, compute provider or compatibility rule: see [extending OLCF](docs/development/extending.md).
* Run OLCF on your own platform (Windows, macOS, a Kubernetes cluster, a Fabric tenant) and tell us what happened. Those reports are
  the most valuable ones right now; see the open items in the [traceability document](docs/compliance/requirements-traceability.md).

## Licence and sign-off (DCO, no CLA)

There is no contributor licence agreement. Instead every commit must carry a **Developer Certificate of Origin** sign-off
([developercertificate.org](https://developercertificate.org/)): by adding it you state that you wrote the change or have the right to submit it under
the project's licence.

```sh
git commit -s -m "storage: reject paths with NUL bytes"      # adds: Signed-off-by: Your Name <you@example.com>
```

Use your real name and a working address. Contributions are licensed under Apache-2.0 (section 5 of the licence). If you want to be listed, add
yourself to [AUTHORS](AUTHORS) in the same pull request. Do not add code you cannot license under Apache-2.0, and do not add Microsoft logos or other third-party brand assets.

## Development setup

You need Python 3.11 or newer (3.13 is what CI uses as the default) and, for anything that starts Spark, a JDK 17 or 21. No compiler is needed. Full
details, including the repository map, are in [docs/development/setup.md](docs/development/setup.md); the short version:

```sh
git clone https://github.com/jddevtime/open-lakehouse-compute-fabric.git && cd open-lakehouse-compute-fabric
uv venv --python 3.13 && . .venv/bin/activate && uv pip install -e ".[dev]"
#   or: python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"

pytest -m "not spark and not e2e and not live and not postgres" -q tests/unit tests/tooling   # fast subset
ruff check src tests sdk scripts examples deploy/scripts                                      # lint (same as CI)
olcf init && olcf serve                                                                       # run it
```

Tests that start a JVM carry the `spark` marker and must be serialised: use the `spark_lock` fixture from `tests/conftest.py` (it takes
`flock /tmp/olcf-spark.lock`) and keep Spark heaps at or below 512 MB. See [testing](docs/development/testing.md).

## Pull request checklist

Before you open a pull request:

- [ ] The change is focused; unrelated refactors are separate pull requests.
- [ ] Every commit is signed off (`git commit -s`).
- [ ] New or changed behaviour has tests under `tests/<area>/`; bug fixes have a test that fails without the fix.
- [ ] `ruff check src tests sdk scripts examples deploy/scripts` is clean and the tests of the areas you touched pass.
- [ ] Every new Python file starts with the two header lines (`python scripts/check_headers.py --fix` adds them) and has a module docstring that says *why* it exists;
      public classes and functions have docstrings (`python scripts/check_docstrings.py`).
- [ ] If you changed the CLI, a setting, a permission, an event, an error code, a health check or an API route, regenerate the references:
      `PYTHONPATH=src python scripts/gen_reference.py && PYTHONPATH=src python scripts/gen_api_docs.py && PYTHONPATH=src python scripts/gen_sdk_docs.py`, and commit the result.
- [ ] `PYTHONPATH=src python scripts/check_docs.py` passes (links, anchors, CLI commands mentioned in the docs, reference freshness).
- [ ] User-visible changes are described under *Unreleased* in [CHANGELOG.md](CHANGELOG.md).
- [ ] You did not commit secrets, tokens, real hostnames or customer data; error messages and logs never contain secret values
      (everything user-visible passes `olcf.core.redaction`).
- [ ] If the change alters a contract in `docs/dev/CONTRACTS.md`, the file is updated in the same pull request and the pull request description says so.

## Engineering rules (the short list)

The binding version is in [`docs/dev/CONTRACTS.md`](docs/dev/CONTRACTS.md) section 0 and [coding standards](docs/development/coding-standards.md).

1. **It must really work.** No stubs, placeholders or faked results. If something cannot be exercised without an external system, implement it
   against the real API contract, cover it with a contract test against a local test server, mark it `live` where applicable, and say so in the docs.
2. **Fail closed.** Unsupported semantics raise an actionable error (the `olcf.core.errors.Unsupported` family); never substitute silently.
3. **Authorise on the server.** Every handler takes `principal: Authed` and calls `principal.require("<permission>", ...)`; objects are re-checked against
   *their own* workspace, never one supplied by the client. Never return secret values.
4. **SQL** only through SQLAlchemy Core with bound parameters; it must run on SQLite and PostgreSQL. **Paths** derived from input are checked for traversal. No `shell=True`.
5. **Host tools** (`git`, `java`, `python`, `pip`) are found only through `olcf.core.hostbin`, which never executes a macOS installer stub.
6. Python 3.11+, type hints, `from __future__ import annotations`, line length 110.

## Review and release

The maintainer reviews pull requests as time allows; small, well-tested changes move fastest. Releases follow the
[release process](docs/development/release-process.md). Questions are welcome as issues labelled `question`.
