# OLCF documentation

Open Lakehouse Compute Fabric, version 0.1.0 (beta), by Justin Dodrill. Apache License 2.0.

OLCF is an open-source lakehouse platform that runs on your own hardware and can also act as external compute for Microsoft Fabric notebooks. Read the [README](../README.md) first for what it is and what state it is in; this page is the map of everything else.

## I want to...

| Goal | Go to |
|---|---|
| find where a number comes from, what a change breaks, what is stale, unused, or sensitive | [Intelligence guide](guides/intelligence.md), the [concepts](concepts/intelligence.md) behind it |
| install it and run a first notebook | [Getting started](getting-started/README.md), then the [first-run tour](getting-started/first-run-tour.md) |
| understand how it works | [Concepts](concepts/README.md) |
| do a specific task (SQL, notebooks, pipelines, SSO, TLS, backup, Fabric, MCP, clusters) | [Guides](guides/README.md) |
| find a command, endpoint, setting, permission, event or error | [Reference](reference/README.md) |
| run it for other people | [Operations](operations/README.md) |
| fix something that does not work | [Troubleshooting](guides/troubleshooting.md), the [FAQ](reference/faq.md) |
| scan a Fabric or Power BI tenant for lineage | [Intelligence: connecting Fabric and Power BI](guides/intelligence.md#connecting-microsoft-fabric-and-power-bi) |
| connect a Fabric notebook | [Fabric integration](guides/fabric-integration.md), and read [the assumptions](guides/fabric-assumptions.md) |
| change or extend the code | [Development](development/README.md) |
| know what is really implemented | [Requirements traceability](compliance/requirements-traceability.md) |
| know why it is built this way | [Architecture decision records](adr/README.md) |
| report a vulnerability | [SECURITY.md](../SECURITY.md) |
| see what the external code review found and what was done about it | [Code review response](dev/CODE_REVIEW_RESPONSE.md) |

## Sections

| Section | Contents |
|---|---|
| [getting-started](getting-started/README.md) | requirements, install routes, quickstart, upgrade and uninstall, first-run tour |
| [concepts](concepts/README.md) | architecture, components and data flow, compute model, workspaces and items, Fabric extension model, Intelligence (asset graph, lineage, governance), security model |
| [guides](guides/README.md) | task-oriented how-tos written and verified by running them |
| [reference](reference/README.md) | generated references (CLI, REST API, SDKs, configuration, permissions, events, errors, health checks, Fabric rules) and hand-written ones (ports, lineage support, glossary, FAQ) |
| [operations](operations/README.md) | runbook, sizing, hardening, production checklist, Kubernetes |
| [development](development/README.md) | setup, repository map, coding standards, testing, extending, release process |
| [adr](adr/README.md) | seventeen architecture decision records |
| [compliance](compliance/requirements-traceability.md) | every section of the original requirements mapped to code, tests and status, with the known limitations |
| [api](api/openapi.json) | the OpenAPI document, generated from the running application |
| [dev](dev/CONTRACTS.md) | engineering contracts, the original requirements and the security-operations notes (maintained by the engineering team; not rewritten for readers). The external code review of 8 October 2026 is [dev/CODE_REVIEW_2026-10-08.md](dev/CODE_REVIEW_2026-10-08.md); the finding-by-finding [response](dev/CODE_REVIEW_RESPONSE.md) says what was reproduced, what changed, which tests pin it and what remains open |
| `images` | screenshots used by the documentation |

## Conventions

* Statements were checked against the code, the command line or a running server.
* Generated pages (the ones in [reference](reference/README.md) that say so) are produced by the scripts in `scripts/` and checked by `scripts/check_docs.py`; edit the code or the generator, not the page.
* Commands are shown for a POSIX shell unless the page is about Windows. `olcf` is the command line; `olcf-worker` is the agent.
* Names that users see (settings, permissions, events, health checks) are exact; the reference pages are the authority.
