# olcf-fabric

Submit a Microsoft Fabric notebook to an on-prem Open Lakehouse Compute Fabric (OLCF). The notebook (or pipeline notebook activity) calls one function; the
control plane re-creates the execution context, runs the **whole notebook** on customer-controlled Spark against OneLake, and returns status, logs and the exit value
into the Fabric cell and the pipeline run.

Runtime label: **Fabric Runtime 2.0 Compatible**. This package is not a Microsoft product and is not endorsed by Microsoft.

* Standard library only. Python >= 3.10 (Fabric notebook runtimes satisfy this).
* Separate distribution from the OLCF server (`olcf-fabric`, import name `olcf_fabric`), so it can be attached to a Fabric Environment without pulling in server dependencies.

## Install

Build the wheel (see `scripts/build_fabric_sdk.sh`; it writes `dist/olcf_fabric-<version>-py3-none-any.whl`) and make it available to Fabric in one of three ways:

1. Attach the wheel to a **Fabric Environment** (Custom libraries) and select that Environment for the notebook. Preferred for pipelines.
2. Upload it to the lakehouse Files area and run `%pip install /lakehouse/default/Files/olcf_fabric-0.1.0-py3-none-any.whl` in the first cell.
3. Publish it to an internal package index and `%pip install olcf-fabric`.

Which one fits depends on your tenant policy (see `docs/guides/fabric-assumptions.md`).

## Bootstrap cell (copy and paste)

`GET /v1/integrations/fabric/snippet` on your control plane returns this cell already filled in for your endpoint and workspace mapping.

```python
# %pip install /lakehouse/default/Files/olcf_fabric-0.1.0-py3-none-any.whl     # or attach the wheel to the Fabric Environment
import olcf_fabric as olcf

olcf.configure(
    endpoint="https://olcf.example.com",
    auth="keyvault",
    keyvault="https://<your-vault>.vault.azure.net/", secret_name="olcf-api-token",
)

olcf.test_connection()   # optional: prints a checklist (DNS, TLS, token, handshake, authorisation, ...)
olcf.run_remote()        # runs the whole notebook remotely; this cell is not part of the remote run
```

Rules the SDK follows for this cell:

* The cell that calls `run_remote()` / `extend()` is removed from the remote run, together with any other cell that only imports `olcf_fabric` and calls it.
* On success the notebook's exit value is handed to `notebookutils.notebook.exit(...)` so a pipeline activity sees it (`exit=False` returns it instead).
* On any other end state `olcf.RemoteExecutionError` is raised, so the pipeline activity fails exactly as a local failure would.
* Values of the notebook's parameter cell (the cell tagged `parameters`) are read from the session and merged under the explicit `parameters={...}` you pass. Nothing else from the interpreter is sent.

## Authentication

The SDK only ever holds the credential of the **caller**. A broker/service-principal secret of the control plane (used for pull mode and OneLake) never belongs in a notebook, a
capsule or a log; the server refuses capsules that contain known secret patterns.

| `auth=`              | How the bearer token is obtained                                                                                       |
|----------------------|------------------------------------------------------------------------------------------------------------------------|
| `"keyvault"`         | `notebookutils.credentials.getSecret(keyvault, secret_name)` holds an OLCF platform token (scope `fabric`) or an Entra access token |
| `"client_credentials"` | Entra app registration: `tenant_id`, `client_id`, secret from Key Vault (`client_secret_name`); token for `audience`   |
| `"notebookutils"`    | `notebookutils.credentials.getToken(audience)`; depends on which audiences the Fabric tenant supports                 |
| `"token"`            | `token=` is used as is. Short-lived tokens only; never commit one                                                      |
| `"auto"` (default)   | The first mode whose inputs are present, in the order token, keyvault, client_credentials, notebookutils               |

Environment fallbacks: `OLCF_ENDPOINT`, `OLCF_TOKEN`, `OLCF_AUTH`, `OLCF_TENANT_ID`, `OLCF_AUDIENCE`, `OLCF_CA_BUNDLE`. Plain `http://` is accepted only for loopback hosts unless
`allow_insecure_http=True` is passed explicitly. A corporate CA is supported with `ca_bundle="/path/ca.pem"`.

## What the signature does and does not give you

Each submission is signed (HMAC-SHA256 over the canonical JSON of the capsule, with a per-submission key derived from the server's challenge, a single-use nonce, a
timestamp window and an idempotency key). That gives **integrity** (the capsule was not modified in transit) and **replay protection** (a captured capsule cannot be run
again). It is **not authentication**: who you are is established by the bearer token (platform token or Entra JWT), and authorisation by the OLCF workspace mapping.

## Calls

```python
run = olcf.run_remote(parameters={"run_date": "2026-10-07"}, wait=False)   # RemoteRun handle
olcf.status(run); olcf.logs(run); olcf.cancel(run)
olcf.finish(run)                                                            # wait, print output, pass the exit value to the pipeline

olcf.run_remote(mode="pull")      # submit by reference: the control plane fetches the definition via the Fabric REST API (needs a broker identity)
olcf.run_remote(source=open("nb.ipynb").read())   # explicit notebook text
```

`mode="auto"` reads the definition with `notebookutils.notebook.getDefinition` (several call spellings are tried defensively) and falls back to pull mode.

Rejections before execution raise `olcf.CompatibilityError` (with the analyzer report: blockers such as `sempy`, `/lakehouse/default` on object storage, or `%pip install` when policy
forbids it) or `olcf.SubmissionRejected`. Every error carries a `remediation` string when the server or the SDK knows one.

## Troubleshooting

`olcf.test_connection()` returns `{ok, checks, handshake}` and prints each step with timing and remediation: DNS, TLS, token, challenge, handshake, latency, server version,
authorisation and the Fabric context seen. It never raises.
