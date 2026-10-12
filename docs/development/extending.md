# Extending OLCF

Three things are extension points that outside packages can plug into **without changing OLCF**: storage providers, compute providers and pipeline activities. They are discovered through Python entry points when the control plane starts. Everything else (a new feature package, health check, setting, item type, CLI command) is a change inside the repository, and the second half of this page shows where.

All plug-ins are trusted code that runs inside the control plane process with its privileges. Install only packages you would run as the server. A plug-in that fails to import or violates a rule is logged and skipped, and can never stop the control plane from starting (storage providers also record the reason in `ProviderRegistry.plugin_errors`, activities in `ActivityRegistry.rejected`).

| Entry-point group | Plug-in is | Registered by | Rules |
|---|---|---|---|
| `olcf.storage_providers` | a `StorageProvider` subclass, or a zero-argument factory that returns an instance | `olcf.storage.providers.ProviderRegistry` | cannot replace a built-in id (`local`, `s3`, `adls`, `onelake`) |
| `olcf.compute_providers` | a `ComputeProvider` subclass with a unique `type` | `olcf.compute.providers.ProviderManager` | a type already registered is refused |
| `olcf.pipeline_activities` | an `Activity` subclass, an instance, or a callable/list that yields them | `olcf.pipelines.activities.ActivityRegistry` | type names are `lower_snake_case`; cannot shadow a built-in type; the activity API major version must match (`ACTIVITY_API_VERSION = "1"`) |

## A pipeline activity

An activity type is a class with a `type`, a JSON Schema for its `typeProperties`, and the hooks of its execution mode:

| Mode | Hooks | Use for |
|---|---|---|
| `blocking` | `execute(actx)` returns the output dict; runs on an engine worker thread | copy, SQL, HTTP |
| `async` | `start(actx)` begins and persists handles with `actx.save_state`; the engine calls `poll(actx)` until it returns a result; `resume` after a crash | long work held elsewhere (jobs, callbacks) |
| `inline` | `execute(actx)` runs on the dispatcher thread; must be quick | `set_variable`, `fail` |
| `container` | derive from `Container`; the engine creates the inner scopes | `if`, `for_each`, `until` |

```python
# olcf_hello/__init__.py
from olcf.pipelines.activities.base import Activity, ActivityContext


class Hello(Activity):
    type = "hello"
    title = "Say hello"
    description = "Returns a greeting."
    mode = "blocking"
    properties_schema = {"type": "object", "properties": {"who": {"type": "string"}}, "additionalProperties": False}

    def execute(self, actx: ActivityContext) -> dict:
        return {"greeting": f"hello {actx.props.get('who', 'world')}"}


ACTIVITIES = [Hello]
```

```toml
# pyproject.toml of the plug-in
[project.entry-points."olcf.pipeline_activities"]
hello = "olcf_hello:ACTIVITIES"
```

This exact plug-in was installed as package metadata next to the source tree and loaded for this page: `build_registry()` reported `plugins: ['hello']`, `rejected: []`, 16 activity types (15 built in plus `hello`). In a pipeline it is then used like any other type, `{"name": "Greet", "type": "hello", "typeProperties": {"who": "docs"}}`, validated against `properties_schema`, listed by `GET /v1/pipelines/activity-types`, and shown in the designer.

Crash semantics decide whether you can use `blocking`. After a crash or failover, a blocking activity is run again only if its class sets `resumable = True` (it must be safe to run twice) or the pipeline marks that activity `policy.idempotent`; otherwise it is failed with the code `interrupted` and never silently executed twice. Set `reusable` to say whether a successful result may be reused when a failed run is resumed, and `sensitive_props` to hide properties. Raise `ActivityError(message, category=..., retryable=False)` for failures that retrying cannot fix. Check `actx.check_cancelled()` or `actx.is_cancelled()` in loops. When the pipeline times an activity out, the engine cancels it and waits (`policy.cancelGraceSeconds`) for it to stop before any retry, so make cancellation prompt and register work that blocks in a call with `actx.on_cancel(callback)` (the `http` activity aborts its socket there). Raise `ActivityCancelled(message, uncertain=True, details=...)` when you were cancelled at a point where you cannot tell whether the other side did the work, and `ActivityError(..., category="<thing>_uncertain")` for the same doubt on a failed call: both are retried only when the activity is idempotent. The interface is public and versioned: a major bump of `ACTIVITY_API_VERSION` rejects older plug-ins instead of misrunning them.

## A storage provider

Subclass `olcf.storage.providers.base.StorageProvider`: set `id`, `label`, `schemes`, `connection_types`, `capabilities` (what the provider can do; anything not advertised fails closed), `connection_fields` (the form the UI shows), and implement `filesystem(root_uri, config, secret)` returning a `FileSystem` (`list`, `stat`, `open_read`, `open_write`, `delete`, `mkdirs`, `move`), `physical_uri(root_uri, rel)`, and, if Spark and delta-rs need credentials, `spark_conf`. Optionally override `claims`, `validate_root` and `validate_config`.

```toml
[project.entry-points."olcf.storage_providers"]
gcs = "my_pkg.gcs:GcsProvider"
```

`ProviderRegistry.plugin_errors` records the reason a plug-in was skipped. Use `tests/data/test_storage_plugins.py` as the model for a contract test, and exercise the provider through `StorageService`, not only the filesystem class.

## A compute provider

Subclass `olcf.compute.providers.base.ComputeProvider`: set `type`, `capabilities`, `execution_model` (`agent` when the application runs on an OLCF worker, `remote` when the controller talks to an external system), `uses_runtimes` and `prepares_spark`, and implement `discover`, `submit`, `status`, `logs`, `cancel`, `ui` and `reconcile` (plus `prepare` for agent-executed jobs, and the optional hooks `scale`, `on_hello`, `maintain`, `health`, `close`). Set `live_verified = False` while the provider is covered only by local test servers: the flag is shown in the API and the documentation, and the built-in `kubernetes` provider carries it.

```toml
[project.entry-points."olcf.compute_providers"]
my_cloud = "my_pkg.provider:MyCloudProvider"
```

A provider type does nothing until an administrator creates an instance of it: `POST /v1/providers` with `name`, `type`, `config` and optionally `secret_id` (permission `compute.manage`); `GET /v1/providers/types` lists the registered types. `validate_config` is called when the instance is created. Make `submit` idempotent with respect to the job id, and make `reconcile` return the true state of every application the provider owns, because the controller calls it after a restart or leader change.

## Changes inside the repository

| You want to add | Do this |
|---|---|
| a feature package | create `olcf/<pkg>/` with `schema.py`, `service.py`, `api.py` and `module.py:install(ctx)`; add it to `olcf/api/modules.py`; register a migration stream named `<pkg>`; add `tests/<area>/`. Missing modules are skipped with a warning at start, but the release gate (`scripts/verify_dist.py`) expects the packages it lists to be in the wheel |
| a REST route | a router in the package's `api.py` that takes `principal: Authed` and calls `principal.require("<permission>", ...)` against the workspace of the object it touches; add new permissions to `olcf/security/rbac.py` and the role matrix; regenerate the references |
| a permission or role | `olcf/security/rbac.py`; then `scripts/gen_reference.py` regenerates the permission matrix, and `tests/security/test_authorization.py` attacks every route with the roles you have defined |
| an item type | `ctx.register_item_hooks("<type>", validate=..., on_update=..., remap_ids=...)` from your module's `install`; definition schemas are listed in `docs/dev/CONTRACTS.md` section 10 |
| a connection type | `ctx.connection_testers["<type>"] = fn` in `install` (a storage provider's `connection_types` do this for you) |
| a health check | `ctx.health.register(HealthCheck(name, category, description, quick, deep=None, critical=False))`; it appears in `GET /v1/admin/health` and in the generated [health checks](../reference/health-checks.md) |
| an event | `ctx.events.publish("<domain>.<noun>.<verb>", payload, workspace_id=..., subject=...)`; the payload is redacted; the generated [events reference](../reference/events.md) finds static `publish("...")` calls |
| a platform setting | a field of the model in `olcf/core/config.py` for file/environment settings; for settings an administrator changes at run time, register a key with the settings registry (`register_settings()` in the owning service, for example `compute/registry.py`) |
| a CLI command | a click command in the matching module of `olcf/cli/commands/`, using the SDK; `scripts/gen_cli_docs.py` documents it; `scripts/check_docs.py` fails if a doc mentions a command that does not exist |
| a Fabric analyzer rule | an entry in `olcf/fabric/analyzer/rules/fabric-2.0.yaml` (a matcher plus class, message and remediation); bump `ruleset_version` and `olcf.version.COMPAT_RULESET_VERSION`, add a case to the corpus under `tests/corpus`, regenerate [Fabric rules](../reference/fabric-rules.md) |
| a `notebookutils` function | `olcf/runtime_lib/notebookutils/` (standard library only), plus its analyzer rule and a test in `tests/fabric/test_shim_runner.py`; anything not implemented must raise a not-supported error with a remedy, never silently do nothing |
| a Spark runtime | a new manifest generated and signed with `scripts/lock_runtime.py` and listed in the runtime catalogue; see the [release process](release-process.md) |

Whatever you add must follow the rules in [CONTRIBUTING.md](../../CONTRIBUTING.md): authorise on the server, fail closed, no secret values in responses or logs, SQL through SQLAlchemy Core with bound parameters on both SQLite and PostgreSQL, no `shell=True`, and tests that fail without the change.
