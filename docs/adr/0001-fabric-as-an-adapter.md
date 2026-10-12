# ADR 0001: Microsoft Fabric is an adapter, not a re-implementation

Status: Accepted

## Context

The product's reason to exist is to let Microsoft Fabric notebooks use compute the customer owns. Two designs were possible: re-implement Fabric's behaviour (its runtime, Warehouse, item types and APIs) so that notebooks run unchanged, or build an independent, general lakehouse compute platform and add a narrow, explicit bridge to Fabric.

Fabric's engines and runtime are Microsoft's; their behaviour changes without notice, and a re-implementation can never be checked against a specification. Claiming parity would be a promise the project cannot keep.

## Decision

OLCF is a general compute platform (workspaces, lakehouses, SQL, notebooks, pipelines, Spark clusters, MCP) that works without any Microsoft service. Fabric support is one module, `olcf.fabric`, that:

* receives a notebook from a Fabric notebook (SDK `run_remote`) or a pipeline activity,
* **analyses it before it runs** with a deterministic rule set (73 rules, `fabric-2.0.yaml`) and blocks, with cell, line and a remedy, whatever the local runtime cannot do,
* runs it on an OLCF runtime labelled "Fabric Runtime 2.0 Compatible", and
* provides a small `notebookutils` shim; anything not implemented raises a not-supported error instead of pretending.

Nothing in the platform makes performance or behavioural claims about Fabric.

## Consequences

* Unsupported constructs are rejected up front rather than failing half way or, worse, running with different semantics. The cost is that some Fabric notebooks cannot be used until they are changed.
* The rule set is a maintained artefact. It has a version (`COMPAT_RULESET_VERSION`) and a test corpus (`tests/corpus`).
* `compute`, `storage` and `catalog` do not import `fabric`; the scheduler reaches it through an optional service lookup. Removing the module leaves a working platform.
* The adapter is covered by contract tests against a local test server; optional tests in `tests/fabric/test_live.py` run against your own tenant when configured ([Fabric assumptions](../guides/fabric-assumptions.md)).

## Alternatives considered

* **Re-implement the Fabric runtime and APIs**: unbounded scope, impossible to check against a specification, and trademark and honesty problems ([TRADEMARKS.md](../../TRADEMARKS.md)).
* **Only a generic Spark platform with no Fabric support**: loses the stated purpose.

## Verified by

`src/olcf/fabric/`, `tests/fabric/`, `tests/corpus/`; the [Fabric extension model](../concepts/fabric-extension-model.md) and the generated [rule list](../reference/fabric-rules.md).
