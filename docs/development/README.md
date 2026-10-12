# Development

For people who change OLCF or build on it. Start with [CONTRIBUTING.md](../../CONTRIBUTING.md) for the rules of contributing (sign-off, checklist, review), then:

| Page | Contents |
|---|---|
| [Setup](setup.md) | clone, install, run a development server, PostgreSQL, the checks to run before a pull request, regenerating the reference |
| [Repository map](repository-map.md) | what lives where, and how the packages depend on each other |
| [Coding standards](coding-standards.md) | language level, headers and docstrings, the behaviour rules (fail closed, authorise on the server, redact, SQL) and why |
| [Testing](testing.md) | the test areas, markers, how to run Spark, PostgreSQL, browser and live tests, known problems in the test setup |
| [Extending OLCF](extending.md) | storage providers, compute providers and pipeline activities as plug-ins; where to add a route, permission, setting, health check or CLI command inside the repository |
| [Release process](release-process.md) | versions, the release checklist, what the workflows do, how to verify a download, runtime manifests, compatibility policy |
| [Release readiness](release-readiness.md) | what has to be done and validated before each kind of release or claim (R01 to R14), the release sequence, the identity blocker |

The engineering contracts between the packages (the binding source for module boundaries, protocol and API conventions) are in [`docs/dev/CONTRACTS.md`](../dev/CONTRACTS.md). The original requirements are in [`docs/dev/MASTER_REQUIREMENTS.md`](../dev/MASTER_REQUIREMENTS.md) and are traced to the code in [requirements traceability](../compliance/requirements-traceability.md). The reasons behind the large decisions are in the [architecture decision records](../adr/README.md).
