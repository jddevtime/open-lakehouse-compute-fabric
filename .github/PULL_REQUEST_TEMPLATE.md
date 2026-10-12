<!-- Thank you for contributing. Security problems: do not open a pull request, see SECURITY.md. -->

## What and why

<!-- What does this change do, and why? Link the issue it closes: "Closes #123". -->

## How it was verified

<!-- Commands you ran and what they showed, e.g. `pytest tests/storage -q`, `ruff check ...`. If there is something you could not run (for example a Fabric tenant, Windows, macOS or Docker), say which. -->

## Checklist

- [ ] Every commit is signed off (`git commit -s`, Developer Certificate of Origin)
- [ ] Tests added or updated (a bug fix has a test that fails without it)
- [ ] `ruff check src tests sdk scripts examples deploy/scripts` is clean; tests of the touched areas pass
- [ ] New Python files have the header and module docstring (`python scripts/check_headers.py`, `python scripts/check_docstrings.py`)
- [ ] Generated references regenerated if the CLI, settings, permissions, events, errors, health checks or API changed
      (`scripts/gen_reference.py`, `scripts/gen_api_reference.py`) and `scripts/check_docs.py` passes
- [ ] CHANGELOG.md updated under *Unreleased* for user-visible changes
- [ ] No secrets, tokens or real hostnames in code, tests, fixtures or logs
- [ ] If a contract changed (`docs/dev/CONTRACTS.md`, REST API, worker protocol, capsule schema, plug-in interface), the contract file is updated and called out here
