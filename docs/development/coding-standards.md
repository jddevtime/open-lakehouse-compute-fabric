# Coding standards

The binding list of engineering rules is section 0 of [`docs/dev/CONTRACTS.md`](../dev/CONTRACTS.md). This page restates them with the reasons, and with the tools that check them. Where a rule has a checker, the checker wins over this page.

## Language and style

* Python 3.11 or newer; the code is developed on 3.13 and CI also runs 3.11. Do not use syntax that only newer versions accept, for example a backslash inside an f-string expression.
* Every module starts with `from __future__ import annotations`; public functions have type hints.
* Formatting and lint are Ruff, configured in `pyproject.toml`: line length 110, target `py311`, rule sets `E`, `F`, `I` (import order), `B`, `UP` and `S` (flake8-bandit). `E501` (line length) is ignored, so the limit is not enforced mechanically; keep to 110 in new code. `S101` (assert), `S311`, `S603`, `S607`, `S110` and `B008` are ignored project-wide. Everything else needs a fix or a targeted `# noqa: <rule>  reason`, never a blanket ignore.
* Run `ruff check src tests sdk scripts examples deploy/scripts`. CI runs the same command; the type check `mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent` must also pass.
* Names are `snake_case`, classes `PascalCase`; identifiers that users see (permissions, events, health checks, settings) follow the patterns in the generated references and are not renamed without a deprecation note in the changelog.
* Time is UTC. Use `olcf.core.timeutil` (`utcnow`, `now_iso`) and pass a clock into code that you need to test. Identifiers come from `olcf.core.ids.new_id(prefix)`, so that an id tells you what it is.

## Files, headers and docstrings

* Every Python file begins with

  ```python
  # SPDX-License-Identifier: Apache-2.0
  # Copyright (c) 2026 Justin Dodrill
  ```

  `python scripts/check_headers.py` reports files that lack it and `--fix` inserts it mechanically. Fabric `notebook-content.py` fixtures in `tests/corpus` are exempt.
* Every module has a docstring that says **why the module exists** and how it fits with its neighbours, not a paraphrase of its file name. Public classes and public functions have docstrings; `python scripts/check_docstrings.py` prints the coverage per package and takes `--min-modules` and `--min-classes` thresholds for a CI gate.
* A docstring is for the reader of the code. Do not put work-package labels, internal ticket names or sections of a requirements document in it; refer to the behaviour, or to a page in `docs/`.
* Comments explain *why* something non-obvious is done (a driver quirk, a race, a Fabric incompatibility), with enough detail to be checked later. Comments that restate the code are removed in review.

## Behaviour rules

1. **It must really work.** No stubs, `pass` placeholders, "TODO: implement", faked results or mock data posing as live data. If something cannot be exercised without an external system, implement it against the real API contract, cover it with a contract test against a local test server, and say so in the documentation.
2. **Fail closed.** An unsupported or unsafe operation raises an actionable error from `olcf.core.errors` (`Unsupported`, `CompatibilityBlocked`, `PolicyDenied`, ...). Never substitute a different behaviour silently. The error carries a stable `code`, a `message`, optional `details` and a `remediation` that says what to do. The API wraps it as `{"error": {"code", "message", "correlation_id", ...}}` with the class's HTTP status; see the [error reference](../reference/errors.md).
3. **Authorise on the server, on the object.** Every route takes `principal: Authed` and calls `principal.require("<permission>", workspace_id=...)`, where the workspace is the one **stored with the object**, never a value the client sent. List endpoints filter by what the principal may read. A new permission is added to `olcf/security/rbac.py` and to the role matrix, and the generated [permissions reference](../reference/permissions.md) is regenerated.
4. **Secrets never leave.** They are written through the secret store, stored encrypted, returned as references (`secret_id`, never values), and resolved on the worker at the moment of use. Anything that goes to a log, an event, a webhook, a support bundle or an error message passes `olcf.core.redaction` (`redact_text`, `redact_obj`; the `RedactingFilter` is installed on the logging system). Add a pattern there if you introduce a new kind of credential.
5. **SQL** goes through SQLAlchemy Core with bound parameters, and runs unchanged on SQLite and PostgreSQL. Never format a value into SQL text. User-supplied SQL for lakehouses goes through `olcf.sql.guard`, which allows read-only statements only.
6. **Paths and URLs** that come from a request are checked: paths against traversal and symbolic links (each storage provider and the workspace import do it), outbound URLs against the SSRF guard (`olcf.pipelines.ssrf`, and the equivalent checks for identity providers and webhooks).
7. **Processes.** Never `shell=True`; pass an argument list. Find host tools (`git`, `java`, `python`, `pip`) through `olcf.core.hostbin`, which never executes a macOS installer stub. Child processes are started with a scrubbed environment, supervised, and terminated with their process group; leaking one is a bug.
8. **Concurrency and restarts.** Control-plane state lives in the database, not in memory. Anything that must happen once per cluster runs only on the leader (`olcf.core.leader`) and must be safe if two replicas briefly both believe they lead (a database constraint or a compare-and-set, not a lock in memory). Anything triggered by a client carries an idempotency key. After a restart, reconcile from the database and from the worker, do not assume.
9. **Persistence and compatibility.** Tables are defined in a package's `schema.py` and changed only by a new migration; never edit an applied migration. Persisted and wire formats carry a version (see [release process](release-process.md#version-numbers)). Do not break an existing document, token or backup without an explicit migration.
10. **No silent network.** There is no telemetry. A new outbound connection must be configurable, off by default unless the feature is useless without it, listed in [ports and networking](../reference/ports-and-networking.md#outbound-connections) and covered by the SSRF guard if its target can be user-supplied.
11. **Logging.** Use `logging.getLogger("olcf.<area>")`. Never log request bodies, tokens or connection strings. Log the correlation id; the API middleware sets it on every request.
12. **The console** (`src/olcf/web/static`) uses no bundler and no runtime CDN. Render through Preact and do not use `innerHTML` (the console code contains none); HTML that comes from notebook outputs goes through `js/notebook/sanitize.js`. New third-party assets are vendored with licence and checksum in `vendor/VENDOR.md`.

## Dependencies

A dependency is added only when the project needs it, with a licence compatible with Apache-2.0 (the release build produces `THIRD_PARTY_NOTICES.md` with `scripts/licenses.py --check`, which fails on unknown or strong-copyleft licences). Add it to `pyproject.toml` (core dependency or an extra). Code that runs inside runtime virtual environments (`olcf/runtime_lib`) and the Fabric SDK (`sdk/fabric`) must keep to the standard library plus what their environment installs. Do not install anything into a developer environment that `pyproject.toml` does not list.

## Tests

Every change has tests that fail without it. See [testing](testing.md) for how, and for what to avoid.

## Commits and pull requests

Commits carry a Developer Certificate of Origin sign-off (`git commit -s`). Write the subject as `area: what changed`, in the imperative and under 72 characters, and explain *why* in the body when it is not obvious. Keep a pull request to one concern. The checklist is in [CONTRIBUTING.md](../../CONTRIBUTING.md#pull-request-checklist).

## Documentation style

Write for an operator or contributor who is competent and in a hurry: say what a thing does, then how to use it, then what can go wrong. State a number only if you measured it or read it from code, and say how. Mark what has not been verified as such. Do not use marketing language, emojis or unexplained abbreviations. Commands in documentation must exist (`scripts/check_docs.py` checks the `olcf` commands against the CLI), and generated pages are regenerated, not edited.
