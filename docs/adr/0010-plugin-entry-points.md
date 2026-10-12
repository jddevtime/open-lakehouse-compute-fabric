# ADR 0010: Extension through Python entry points, three groups

Status: Accepted

## Context

Storage backends, compute backends and pipeline step types are the places where users legitimately need more than the project ships. A plug-in mechanism should not require patching OLCF, should not let a faulty plug-in take the server down, and should be small enough to keep stable.

## Decision

Plug-ins are Python packages that declare entry points in one of three groups, discovered when the control plane starts:

| Group | Plug-in | Rules |
|---|---|---|
| `olcf.storage_providers` | `StorageProvider` subclass or zero-argument factory | cannot replace a built-in provider id |
| `olcf.compute_providers` | `ComputeProvider` subclass with a unique `type` | a duplicate type is refused |
| `olcf.pipeline_activities` | `Activity` subclass, instance, or a callable or list that yields them | `lower_snake_case` type; cannot shadow a built-in; the activity API major version must match (`ACTIVITY_API_VERSION = "1"`) |

A plug-in that fails to import or breaks a rule is logged and skipped, and never prevents start-up. Storage providers also record the reason in `plugin_errors`, and the activity registry in `rejected`; compute providers are logged only. A compute provider type does nothing until an administrator creates an instance with `POST /v1/providers`. Everything else (feature packages, routes, permissions) is deliberately not pluggable and changes inside the repository ([extending OLCF](../development/extending.md)).

## Consequences

* **Plug-ins are trusted code** that runs in the control plane process with its privileges. There is no sandbox. The documentation says so.
* The three interfaces are public and versioned; changing them is a breaking change and needs an API version bump for activities.
* Plug-ins are not covered by the project's tests; the repository ships contract tests to copy (`tests/data/test_storage_plugins.py`).

## Alternatives considered

A general hook system (rejected: unstable surface); out-of-process plug-ins over RPC (rejected for now: cost, and the worker side already provides process isolation for user code).

## Verified by

`src/olcf/storage/providers/`, `src/olcf/compute/providers/`, `src/olcf/pipelines/activities/`; a test "hello" activity was installed and loaded for the documentation; `tests/data/test_storage_plugins.py`.
