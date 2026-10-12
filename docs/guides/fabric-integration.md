# Run Fabric notebooks on your own Spark

This guide sets up the Fabric compute extension end to end: what an administrator configures on the control plane, what goes into the Fabric notebook, how a Fabric pipeline uses it, and how to find out why it does not work. The design is in the [Fabric extension model](../concepts/fabric-extension-model.md); [Fabric assumptions](fabric-assumptions.md) lists the Microsoft behaviours this relies on and how to confirm each on your own tenant. Run the connection test first (`olcf fabric test`, or `olcf.test_connection()` in a notebook), because tenant settings, roles and quotas differ between organisations.

The package is not a Microsoft product and is not endorsed by Microsoft. The runtime is labelled **Fabric Runtime 2.0 Compatible**.

## What you need

| Item | Notes |
|---|---|
| A reachable control plane | Fabric's notebook runtime must be able to open HTTPS to `server.public_url`. Put the control plane behind TLS and a name Fabric can resolve ([TLS and mTLS](tls-and-mtls.md), [ports](../reference/ports-and-networking.md#reverse-proxy)) |
| A runtime | `olcf runtime install` (the default family `fabric-2.0-compatible` maps to `olcf-rt-2.0.0`) |
| An OLCF workspace per Fabric workspace | the destination of the submitted notebooks |
| A credential for the notebook | an OLCF API token held in Azure Key Vault, or an Entra app registration whose tokens the control plane accepts |
| For pull mode and OneLake | a broker identity: an OLCF `onelake` connection whose secret holds an Entra service principal |

## 1. Configure the control plane

The integration is configured with one document (`PUT /v1/integrations/fabric`, the Fabric page in the UI, or `olcf fabric configure FILE`). Everything defaults to the strict value; the permissions are `integration.fabric.read` and `integration.fabric.manage` (administrators).

```json
{
  "allowed_tenants": ["11111111-2222-3333-4444-555555555555"],
  "audiences": ["api://olcf-docs-example"],
  "workspace_map": [
    {"fabric_workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
     "olcf_workspace": "Sample Retail", "label": "Retail (Fabric)"}
  ],
  "default_policy": {"queue": "default", "allow_pip_install": false, "allow_shell": false}
}
```

```bash
olcf fabric configure fabric.json     # OK: configuration saved
olcf fabric status                    # the readiness checklist
```

This document was loaded into the sample installation while writing this guide; the checklist then showed enabled, tenants, audience and workspace mapping as satisfied, and the broker connection, a reachable public URL, a signing key (it is created on the first challenge), a handshake from a Fabric notebook and a successful submission as outstanding.

| Field | Meaning |
|---|---|
| `enabled` | master switch (also the setting `fabric.enabled`) |
| `allowed_tenants` | Entra tenant ids from which Entra tokens are accepted |
| `audiences` | the app id or App ID URI of the registration that Entra tokens must be issued for |
| `workspace_map` | Fabric workspace id to OLCF workspace (and optionally a tenant). A submission from an unmapped workspace is rejected with `fabric_workspace_not_allowed` |
| `lakehouse_map` | run against an existing OLCF lakehouse instead of the Fabric one, for example a local copy of the data |
| `broker_connection` | id of the `onelake` connection that holds the broker service principal. Needed for pull mode, for OneLake data access and for `fabric test` |
| `default_runtime`, `default_policy` | the runtime family and the queue, priority, timeout, retries and resource caps. `allow_pip_install` and `allow_shell` exist **only here**; a notebook can narrow them, never widen them |
| `secret_grants` | the `(vault, name)` pairs that `notebookutils.credentials.getSecret` may return, each bound to an OLCF secret |
| `enforce_data_scope` | reject OneLake references to Fabric workspaces that are not mapped (default on) |
| `auto_create_lakehouses` | create an OLCF lakehouse item the first time a mapped Fabric lakehouse is referenced |
| `network` | proxy, CA bundle, timeouts and alternative endpoints (`authority_host`, `fabric_api`, `onelake_dfs`) for sovereign clouds and proxies |
| `pull_timeout_seconds`, `sdk_hints` | pull mode timeout; the definition formats the control plane requests and the SDK is told to prefer |

The `fabric.*` settings (`fabric.capsule_max_age_seconds` 600, `fabric.capsule_max_bytes` 64 MiB, and others) are in the [configuration reference](../reference/configuration.md).

### The broker identity

Pull mode and OneLake access use a service principal. Create an app registration in Entra, create an OLCF connection of type `onelake` holding its tenant id, client id and secret, and set `broker_connection` to the connection's id. As documented by Microsoft when this was written, the principal needs the Fabric tenant settings "Service principals can use Fabric APIs" and "Users can access data stored in OneLake with apps external to Fabric", and membership of every workspace it must reach (Viewer or higher). Give it the least privilege that works: the Spark OAuth settings for OneLake, including the client secret, reach the worker in the job spec, and code in a job can read its own Spark configuration ([the known gap](fabric-assumptions.md#4-secrets)).

### Who may call

Two kinds of credential are accepted by `/v1/integrations/fabric/*`, and by nothing else for Entra tokens:

* An **OLCF personal access token** with the `fabric` scope, kept in Azure Key Vault. Simple, but the token's owner decides the tenant claim, so `allowed_tenants` is only a consistency check and the real boundary is the token's workspace binding.
* An **Entra access token** for the OLCF API registration. The tenant is proven by the token. The control plane maps it to a principal `fabric:<tenant>:<object id>` that holds the `ServicePrincipal` role only in the workspaces mapped for its tenant.

Either way the caller can submit jobs and read or cancel them in the mapped workspaces and nothing else.

## 2. Check the analyzer on a notebook

Before moving anything, ask the control plane what it thinks of a notebook. No job is created.

```bash
olcf fabric analyze notebook.ipynb                 # formats: ipynb, fabric_git_source, py
olcf fabric analyze notebook.ipynb --lakehouse remote
```

An ordinary notebook (from the compatibility corpus) gives:

```text
runtime_family:             fabric-2.0-compatible
blocked:                    no
summary:                    4 construct(s): 1 native-portable, 3 compatibility-shim
CLASS               RULE              CELL  LINE  MESSAGE
compatibility-shim  FAB-PATH-REL      1     1     Resolved against the job's default lakehouse by the Spark s...
native-portable     FAB-IMP-BASELINE  2     2     Part of the OLCF runtime baseline.
compatibility-shim  FAB-NBU-NOTEBOOK  3     5     exit() returns the value as the job's exit value; ...
```

and a notebook with a shell escape is blocked with the cell, the line and the way out:

```text
blocked:                    yes
summary:                    1 construct(s): 1 unsupported; 1 blocker(s)
unsupported  FAB-MAGIC-SHELL  0  1  Shell execution on workers is blocked...  Use Python/Spark APIs, or ask an admi...
```

The six rule classes and all 73 rules are in the [rule catalogue](../reference/fabric-rules.md). `--lakehouse local|remote` answers the what-if "what if the default lakehouse lives here?", which changes how `/lakehouse/default` paths are treated.

## 3. Install the SDK in Fabric

`olcf-fabric` is a separate, standard-library-only package (import name `olcf_fabric`, Python 3.10 or newer) so that it can be attached to a Fabric Environment without server dependencies. Build the wheel on any machine with the repository:

```bash
scripts/build_fabric_sdk.sh           # writes dist/olcf_fabric-<version>-py3-none-any.whl
```

Then use one of: attach the wheel to a Fabric **Environment** as a custom library (preferred for pipelines), upload it to the lakehouse `Files` area and `%pip install /lakehouse/default/Files/olcf_fabric-<version>-py3-none-any.whl` in the first cell, or publish it to an internal index. Which of these your tenant allows is your decision.

## 4. The notebook cell

`olcf fabric snippet` (or the Fabric page, or `GET /v1/integrations/fabric/snippet`) prints the cell with your endpoint filled in:

```python
import olcf_fabric as olcf

olcf.configure(
    endpoint="https://olcf.example.com",
    auth="keyvault",
    keyvault="https://<your-vault>.vault.azure.net/", secret_name="olcf-api-token",
)

olcf.test_connection()   # optional: prints a checklist
olcf.run_remote()        # runs the whole notebook remotely; this cell is not part of the remote run
```

`olcf fabric snippet --auth token|client_credentials|notebookutils` produces the other authentication forms. Never put a token or client secret in the cell: keep it in Key Vault and let the SDK read it with `notebookutils.credentials.getSecret`.

`run_remote()` sends the whole notebook, waits, streams the remote log into the cell output and, on success, passes the notebook's exit value to `notebookutils.notebook.exit` so that a pipeline sees it. On any other end state it raises `RemoteExecutionError`, which fails the pipeline activity like a local failure. Frequently used arguments (the full list is in the [SDK reference](../reference/fabric-sdk.md)):

| Argument | Meaning |
|---|---|
| `parameters={...}` | values for the parameters cell; only declared parameters travel |
| `wait=False` | return a `RemoteRun` at once; use `.status()`, `.logs()`, `.cancel()`, `.wait()`, or `olcf.finish(run)` to apply the success or failure semantics later |
| `mode` | `auto` (push the notebook text, else pull it), `inline` (never pull) or `pull` (identifiers only) |
| `queue`, `priority`, `timeout_seconds`, `retries`, `resources`, `placement` | job policy, bounded by the administrator's `default_policy` |
| `runtime_family`, `spark_conf`, `dependency_lock` | the runtime request (only approved Spark settings are honoured) |
| `validate=True` | run the analyzer first through `/validate` so that a blocker is reported before compute is used |
| `idempotency_key` | the same key never starts two runs |

```python
run = olcf.run_remote(parameters={"load_date": "2026-10-01"}, wait=False, queue="default", priority=70)
print(run.status()["state"])
run.wait()                       # streams the log; raises RemoteExecutionError unless it succeeded
```

### From a Fabric pipeline

Put the notebook in a Fabric pipeline's Notebook activity as usual. The notebook's `run_remote()` cell runs inside Fabric; the activity ends when the remote run ends, succeeds with the exit value, or fails with the remote error. Parameters passed by the pipeline arrive in the notebook's parameters cell and are forwarded.

## 5. Observe and operate

* `olcf fabric submissions [--state S] [--limit N]` lists what arrived from Fabric, including rejected submissions, each with the reason. The submission's job is an ordinary job (origin `fabric`) in **Operate, Jobs**, with logs, diagnostics and the analyzer report.
* `olcf fabric test` (or the Connection tab) runs seven steps, each with duration, detail and remediation: DNS, TLS, proxy, Entra token, Fabric API, OneLake, and the inbound path. A step that depends on the broker identity is skipped with the reason when none is configured. Example from the sample installation, which has a proxy but no Internet access and no broker:

  ```text
  STEP          OK   DETAIL
  DNS           yes  a proxy is configured; target names are resolved by the proxy
  TLS           no   Entra ID: ProxyError; Fabric API: ProxyError; ...
  proxy         yes  proxy ... accepts connections
  Entra token   no   no broker connection is configured
  Fabric API    no   skipped: no Entra token
  OneLake       no   skipped: no Entra token
  inbound path  no   JWKS of tenant ... could not be fetched
  ```

  The last step also reports the age of the last handshake from a Fabric notebook. `olcf.test_connection()` in the notebook creates that handshake, and it is the only proof that Fabric can reach the control plane.
* Metrics are on the Metrics tab and in the Prometheus output as `olcf_fabric_*` ([observability](observability.md)).
* Rotate the signing material with `POST /v1/integrations/fabric/signing/rotate` after a suspected leak. 

## 6. Troubleshooting

| Symptom | Cause and remedy |
|---|---|
| `fabric_not_configured` (409) | the integration is disabled, the mapped OLCF workspace no longer exists, or pull mode was needed and no broker connection is configured. The message and remediation say which; `olcf fabric status` shows the open checks |
| `entra_token_invalid` (401) | wrong tenant, audience, expired token, or JWKS unreachable from the control plane. Compare the token's `tid` and `aud` with `allowed_tenants` and `audiences`; run `olcf fabric test` |
| `fabric_tenant_not_allowed` (403) | add the tenant id to `allowed_tenants` |
| `fabric_workspace_not_allowed` (403) | the Fabric workspace id has no `workspace_map` entry (or its entry is bound to another tenant) |
| `capsule_expired` | the notebook machine's clock is off by more than `fabric.capsule_max_age_seconds` (600) |
| `capsule_replay` (409) | a different capsule reused a challenge nonce; a nonce works once. An exact resend of the same signed capsule is not an error, it returns the existing submission |
| `capsule_signature_invalid` | the body changed after signing (a proxy that rewrites bodies), or the signing key was rotated between challenge and submit |
| `capsule_contains_secret` | the scanner found a credential-like value in the source or parameters. Move it to Key Vault |
| `capsule_too_large` (413) | the capsule exceeds `fabric.capsule_max_bytes`. Keep data out of the notebook |
| `compatibility_blocked` / `CompatibilityError` | the analyzer blocked the notebook before it ran. The report has the cell, line and remedy for each blocker; see `olcf fabric analyze` |
| Pull mode fails | no broker connection, or the principal is not a member of the workspace (see `olcf fabric test`). The pull code follows the `Location` and `Retry-After` headers of the long-running `getDefinition` operation |
| The job is rejected as infeasible on a small machine | the requested executors do not fit the nodes. Lower the request with `resources`, or place it with `placement={"provider": "local"}` |
| OneLake reads fail in the job | check the connection's service principal and the OneLake tenant setting above; `olcf fabric test` OneLake step |

Error codes are listed in the [errors reference](../reference/errors.md).

## What this does not do

Whole-notebook execution is the supported mode; offloading single cells is not implemented. A Fabric Warehouse is a different engine and is out of scope. Fabric features without an OLCF equivalent are ignored or rejected as their rule says, never silently replaced. Performance relative to Fabric is not claimed.
