# Fabric integration: assumptions and limits

Author: Justin Dodrill. Applies to the `fabric` work package (`src/olcf/fabric`, `sdk/fabric`, `src/olcf/runtime_lib/notebookutils`).

The runtime label is **Fabric Runtime 2.0 Compatible**. The product is not Microsoft Fabric, is not endorsed by Microsoft, and never reports itself as Fabric
(`notebookutils.runtime.context["productType"]` is not `Fabric`).

## 1. Test coverage

Everything that talks to Microsoft follows the **documented** shape of the APIs. The test suite covers those shapes with contract tests against a local test server (`tests/fabric/fabric_fakes.py`: Entra token endpoint and JWKS, Fabric REST
including `getDefinition` as a long-running operation with `429`/`Retry-After`, OneLake DFS, and a local `notebookutils` module).

| Layer | Evidence | Coverage |
|---|---|---|
| Capsule schema, canonical JSON, signing, replay, size limit, secret scanner | unit and end-to-end tests | automated |
| Control plane, worker agent, runner, shim, analyzer, SDK together | `tests/fabric/test_e2e_*.py` (real server, real agent, real runner, real Jupyter kernel; real Spark in `test_e2e_spark.py`) | automated |
| Entra JWT validation (issuer, audience, tenant, expiry, signature, key rotation) | locally generated RSA keys and a local JWKS endpoint | contract tests |
| Fabric REST `getDefinition` flow, throttling, failures | local Fabric test server | contract tests |
| OneLake access with the broker service principal | local DFS endpoint; the Spark ABFS OAuth settings are generated and unit-tested | contract tests |
| `notebookutils.notebook.getDefinition`, `credentials.getSecret`, `credentials.getToken`, `runtime.context` inside a Fabric notebook | local `notebookutils` module | confirm with `olcf.test_connection()` in your notebook |
| Pipelines (notebook activity) passing parameters and reading `exitValue` | the exit value is handed to `notebookutils.notebook.exit`; the pipeline side runs inside Fabric | confirm with a pipeline run in your tenant |
| Intelligence discovery of a tenant: Fabric REST, OneLake Delta logs, the Power BI admin scanner, scanner-result import ([2.6](#26-intelligence-discovery-the-scanner-and-the-fabric-and-power-bi-rest-apis)) | the test suite's Fabric server, which follows the documented shapes | confirm with the connection test against your tenant |
| Delta MERGE on the cached test runtime | the cached test runtime has no Delta jars; the corpus case is analysis plus an independent delta-rs golden, and its notebook run is skipped | production runtimes include the jars |

Optional tests in `tests/fabric/test_live.py`, marked `live`, run against your own tenant when the `OLCF_LIVE_FABRIC_*` variables are set (section 8); they are not part of the default run.

## 2. Assumptions about Microsoft behaviour (all coded defensively)

### 2.1 `notebookutils.notebook.getDefinition`

The public documentation of `notebookutils` does not pin down this call for the notebook runtime. The SDK (`olcf_fabric.fabric.acquire_definition`) therefore:

* looks the function up with `getattr` and treats its absence as "not available";
* tries these spellings in order, per requested format: `(name, workspaceId=..., format=...)`, `(name, workspace_id, format)`, `(name, format=...)`; without a format `(name, workspaceId=...)`, `(name)`;
  a `TypeError` means "wrong spelling, try the next", any other exception means "this format failed, try the next format";
* accepts a `str`, `bytes`, a dict that is itself an ipynb document, or a REST-style dict with `definition.parts[]` whose `notebook-content.*` part is base64 (`InlineBase64`);
* asks for `fabricGitSource` then `ipynb` by default (the SDK order is `olcf.configure(definition_formats=[...])`; the server-side `sdk_hints.definition_formats` setting orders the formats
  the control plane requests in pull mode and is returned to the SDK in the challenge and handshake responses for diagnosis);
* when nothing works, falls back to **pull mode**: only identifiers travel in the capsule and the control plane fetches the definition with the Fabric REST API
  (`POST /v1/workspaces/{id}/notebooks/{id}/getDefinition?format=...`, long-running operation, `Location` / `Retry-After`, then `/result`).

If your tenant behaves differently, set the formats, or force `mode="pull"` / `mode="inline"` with `source=` (the notebook text) in `olcf.run_remote`.
The unsaved state of the notebook is **not** what is sent: `getDefinition` returns the saved item. Run the cell after saving.

### 2.2 `notebookutils.credentials.getToken(audience)`

Fabric documents `getToken` for a fixed set of audiences (storage, Power BI, Key Vault, Kusto). A token for a custom API app registration (`api://<app-id>`) is
**not** documented as obtainable that way and probably is not. So `auth="notebookutils"` is only offered as an option and clearly says so when it fails; the supported
patterns are:

1. a platform token (`olcf_pat_...`, scope `fabric`) stored in **Azure Key Vault** and read with `notebookutils.credentials.getSecret` (`auth="keyvault"`, the default in the snippet);
2. an Entra app registration with client credentials, the client secret in Key Vault (`auth="client_credentials"`); the control plane validates the resulting JWT
   (`allowed_tenants`, `audiences`, signature through the tenant JWKS).

Both are contract-tested. Whether Key Vault references are readable from your notebook (workspace identity or user permissions on the vault) is a tenant matter.

### 2.3 Runtime context keys

`notebookutils.runtime.context` keys differ between runtimes. The SDK reads `currentNotebookName`, `currentNotebookId`, `currentWorkspaceId`, `currentWorkspaceName`,
`defaultLakehouseId/Name/WorkspaceId`, `userName`, `isForPipeline`, `currentRunId`, `rootRunId`, `parentRunId`, `activityId` when present, tolerates absence, removes anything
that looks like a credential (`token`, `secret`, `password`, `key`, ...), and sends the list of keys it saw in the handshake (`context_keys_seen_in_handshakes` in the metrics) so you can see what your tenant actually provides.

### 2.4 Service principal and tenant settings (pull mode and OneLake)

For pull mode and for Spark reading OneLake, the control plane uses a **broker** service principal (an OLCF `onelake` connection; the secret is in the OLCF secret store).
As documented by Microsoft at the time of writing, it needs:

* the Fabric admin setting "Service principals can use Fabric APIs" enabled for a group containing it;
* the Fabric admin setting "Users can access data stored in OneLake with apps external to Fabric" enabled;
* membership (Viewer or higher; Contributor if it must create folders) in every Fabric workspace it is meant to reach.

`POST /v1/integrations/fabric/test` diagnoses these step by step (DNS, TLS, proxy, Entra token, Fabric API, OneLake, inbound path) and prints a remediation per failure.
The remediation texts follow Microsoft's documentation; where your tenant differs, your tenant is right.

### 2.5 OneLake paths and drivers

Spark reaches OneLake through `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/...` with the ABFS OAuth client-credentials settings generated by
`StorageService.spark_conf`. Workspace and lakehouse **ids versus names** in URLs, regional endpoints and the Hadoop/`hadoop-azure` versions in a customer runtime can all
differ; the endpoint is configurable (`network.onelake_dfs`). The `/lakehouse/default` FUSE mount of Fabric does not exist on the workers: the analyzer rewrites the literal for
lakehouses that are local directories (rule `FAB-PATH-MOUNT-LOCAL`) and **blocks** it for lakehouses on object storage (`FAB-PATH-MOUNT-REMOTE`).

### 2.6 Intelligence discovery: the scanner and the Fabric and Power BI REST APIs

OLCF Intelligence ([concept](../concepts/intelligence.md), [guide](intelligence.md#connecting-microsoft-fabric-and-power-bi)) has a second Fabric client, separate from the compute adapter above: the `fabric` discovery source (`src/olcf/intelligence/discovery/fabric.py`). It follows Microsoft's documentation and is covered in the test suite by a local HTTP test server (`tests/fabric/fabric_fakes.py`). The statements in this section follow Microsoft's documentation ([release readiness](../development/release-readiness.md), R15); read every item as "the documentation says" and confirm it on your tenant with the acceptance matrix below.

What it assumes, so that you can check each against your tenant:

* **Identity.** An Entra service principal (the *metadata identity*) with a client secret or a certificate, whose credentials are held in the OLCF secret store. An optional second principal (the *sampling identity*) is the only one that may read data.
* **Tenant settings.** For the Fabric APIs: "Service principals can use Fabric APIs" for a security group that contains the principal. For the Power BI admin scanner: "Service principals can access read-only admin APIs" and the two "Enhance admin APIs responses" settings (detailed metadata; DAX and mashup expressions) for the same or another group. Changes can take about 15 minutes to apply. The connectivity test names the setting when a step fails; the setting names are the ones in the documentation at the time of writing and Microsoft renames them.
* **Roles.** The principal must be a member of every workspace to be read: Viewer is enough to list items and tables, Contributor is assumed to be needed for `getDefinition` of notebooks, pipelines, semantic models and reports. A workspace it is not a member of is not discovered by the item calls (the admin scanner sees tenant-wide, if the settings above allow it).
* **No admin consent permissions.** The assumption (from Microsoft's documentation of the admin APIs) is that the scanner needs the tenant settings above and *no* admin-consent API permissions on the app registration. This statement is Microsoft's; confirm it on your tenant with the scanner permission probe of the connectivity test.
* **OneLake.** The metadata identity reads Delta logs through the OneLake DFS endpoint, which requires "Users can access data stored in OneLake with apps external to Fabric" and read access to the lakehouse. Only `metaData` and `protocol` actions of the Delta log are parsed; `add` actions are skipped unread. Without that access a table is reported without columns and a `permission_denied` gap.
* **Calls and quotas.** `GET /v1/workspaces`, `GET /v1/workspaces/{id}/items`, the lakehouse Tables API (assumed unavailable for schema-enabled lakehouses, with a fallback that lists `Tables/` through OneLake), `getDefinition` as a long-running operation (`202`, `Location`, `Retry-After`), and the admin `workspaces/modified`, `getInfo`, `scanStatus` and `scanResult` calls. The scanner is assumed to accept 100 workspaces per `getInfo` request and 500 requests per hour (`intelligence.fabric_scanner_batch_size`, `intelligence.fabric_getinfo_requests_per_hour`), with at most 16 scans in flight. A `429` is retried after `Retry-After`; a wait longer than `intelligence.fabric_max_wait_seconds` ends the scanner phase with a gap instead of sleeping.
* **Response shapes.** The field names of the workspace, item, table and scanner responses, and the part names and formats of the definitions (`.ipynb` and `fabricGitSource` notebooks, pipeline JSON, TMDL and legacy semantic models, PBIR and legacy reports), are taken from the documentation and from examples. A field that differs becomes a gap or a missing edge, not an error that stops the scan.
* **GUID versus name in URLs.** A OneLake URL can name the workspace and lakehouse by GUID or by friendly name. The two forms are different strings to OLCF (locations are normalised for case and trailing slashes only), so a table reached by both forms is a placeholder plus an `unresolved_reference` gap, not a wrong merge.
* **Endpoint overrides (`config.network`).** The authority host, the Fabric, Power BI and OneLake endpoints, a proxy, a CA bundle and the timeout can be overridden per source; endpoints must be `https`, must not carry credentials and must not point at a loopback, private, link-local or cloud-metadata address (checked when the source is saved and again before it is used; the platform setting `intelligence.allow_private_endpoints`, default `false`, lifts the loopback / private block and allows plain `http` to such addresses for private-link gateways and test doubles, and the metadata endpoint stays blocked either way). Setting them needs `integration.fabric.manage`. While the setting is on, whoever holds that permission can point the control plane at an internal address, and the service principal's credential is sent to the endpoint; behind a proxy the proxy resolves the destination, so only a local name check is possible. Treat the permission and the setting accordingly ([security model](../concepts/security-model.md#intelligence-discovery-lineage-and-sampling)).
* **Connectivity test.** Eight steps (resolve credentials, acquire a Fabric token, list workspaces, acquire a Power BI token, scanner permission probe, item read probe, definition read probe, sampling identity probe). The remediation texts follow Microsoft's documentation.

**Acceptance matrix for your own tenant.** Use a dedicated test tenant and workspace set, one least-privilege *metadata* identity and a separate *sampling* identity, and compare what OLCF discovers against an independently known inventory:

* list pagination (more workspaces / items / tables than one page, and a listing that exceeds `DEFAULT_MAX_PAGES`, which must end the scan `partial`);
* `getDefinition` as a long-running operation (the `202` polling path), throttling (`429` with `Retry-After`), token refresh (`401` once);
* lakehouses with and without schemas enabled (Tables API versus the OneLake fallback), OneLake `_delta_log` variants (JSON commits only, checkpoints, multi-part checkpoints);
* scanner permission failures (tenant setting off, workspace not visible to the admin API) and a workspace visible to the scanner but absent from the metadata identity's own listing;
* `modifiedSince` accepted at 5, 35 and 60 minutes and rejected below 30 minutes (this is the documented window);
* with sampling off, confirm from the OneLake storage diagnostic logs that no data file was read.

Record every missing or unsupported object explicitly.

A run submitted from a Fabric notebook through the compute adapter is tied to the notebook and tables a tenant scan found by the Fabric ids the job carries (tenant, workspace and notebook GUIDs, the GUIDs of the attached Fabric lakehouse). This assumes that the identifiers the Fabric SDK sends in the capsule are the real GUIDs of the notebook session (the control plane takes them from the job's origin, not from claimed default-lakehouse fields), that the OneLake URL form of the scan and of the run agree (GUID versus friendly name, see above), and that the Entra tenant of the scan is the tenant of the run.

## 3. What the signature does and does not protect

Every submission carries `HMAC-SHA256(key, "olcf.capsule.v1\0" + canonical_json(capsule))`, where the key is derived per submission from a challenge and a server secret,
plus a single-use nonce, a timestamp window and an idempotency key.

* **Protects**: integrity of the capsule between the SDK and the control plane (a modified capsule is rejected with `capsule_signature_invalid`), replay (a nonce works once;
  a stale or future-dated capsule is rejected), and size (`capsule_max_bytes`, checked before parsing).
* **Does not protect**: identity. The per-submission key is given to any caller that is already authenticated, so a valid signature means "the body is what the holder of
  the key signed", not "this person wrote it". Identity and authorisation come from the bearer token (platform token or Entra JWT), the tenant and audience allow-lists and the
  principal's workspace binding. A stolen token can fetch keys and sign anything its principal may submit: revoke the token.
* **Idempotency**: an exact resend of the same signed capsule (same submission id, same digest) returns the existing submission with `replayed: true`; this is how a lost
  response is retried safely. A *different* capsule that reuses a nonce is rejected with `409 capsule_replay`. A capsule signed with another principal's key fails verification.

## 4. Secrets

* The broker service-principal secret exists only in the OLCF secret store. It is never in a notebook, a capsule, a log line, a submission row, an event or an audit entry;
  the scanner (`olcf.fabric.capsule.find_secrets`) refuses capsules that contain known secret patterns and the end-to-end tests scan every API surface, the worker support
  bundle and the agent state for the known broker secret.
* **Known gap**: when a job uses a OneLake lakehouse, the Spark ABFS OAuth settings (including the client secret) must reach the worker. They travel in the job spec file
  (`storage_conf`, mode 0600, removed when the job ends). They are not in the credential-free lakehouse descriptor, the logs or the output notebook, but user code running in
  the job can read its own Spark configuration (`spark.conf.get(...)`): a notebook author who can run a job on a OneLake-backed lakehouse can read that secret. Use a
  broker principal with the least privilege (read-only on the needed workspaces) or run OneLake jobs only in trusted workspaces. Short-lived delegated tokens are the way to remove this gap.
* `notebookutils.credentials.getSecret(vault, name)` only returns secrets that an administrator granted explicitly (`secret_grants`), delivered to the job as environment
  variables by the platform. Anything else fails with "was not granted". Environment variables are visible to the job's own processes.

## 5. Policy limits

* `%pip install`, `!pip`, `pip.main` and `subprocess` pip calls are blocked by the analyzer unless `default_policy.allow_pip_install` is set. The **runtime** has no package-index
  policy yet: a `%pip install` of a package that is not already in the runtime is always refused at run time (the environment is immutable, hash-pinned), even when the analyzer
  allows it. `%pip install` of an already installed package at a compatible version is a no-op. Declare dependencies in an Environment item instead.
* Shell escapes (`!cmd`, `%%sh`, `os.system`, `subprocess(shell=True)`) are blocked by the analyzer unless `allow_shell` is set. The analyzer is a static check: constructed commands
  (`exec`, `eval`, `getattr` tricks) are reported as warnings, not proven absent. The real isolation is the worker (process user, scrubbed environment, workspace-scoped storage).
* **Data scope**: `enforce_data_scope` rejects OneLake references to Fabric workspaces that are not mapped. Paths computed at run time cannot be checked statically; the broker
  principal's permissions in Fabric are the real boundary. Give it only the workspaces you intend to expose.

## 6. Authorisation notes

* Entra tokens are accepted **only** by `/v1/integrations/fabric/*`; every other API route answers 401 for them. A mapped Entra principal is created per `tid`/`oid`, receives the
  `ServicePrincipal` role only in the OLCF workspaces mapped for its tenant, and its tokens carry the `fabric` scope.
* A **platform token** has no `tid` claim, so for those callers the tenant is whatever the capsule claims. `allowed_tenants` is then a consistency check, not a security
  boundary; the boundary is the token's workspace binding. Use Entra tokens when the tenant must be proven.
* Audience equivalence (`api://<guid>` and `<guid>`) is applied only to GUID application ids.
* Jobs created for a submission run as an internal principal with the narrow `fabric-internal` scope (job submit/read/cancel, item create/read for lakehouse attachment, secret and
  connection use) limited to the mapped OLCF workspace. Rejected submissions are stored with no workspace and are therefore visible only to principals with `integration.fabric.read`
  (administrators); the caller gets the rejection in the HTTP response.

## 7. Platform behaviour worth knowing

* On a single-node install the foundation planner treats Spark jobs as standalone-cluster jobs and rejects them as infeasible when the executor sizing does not fit; Fabric jobs
  therefore pass `placement={"provider": "local"}` in tests, and administrators should set an equivalent default for the Fabric queue.
* Attaching a lakehouse item runs a best-effort `mkdirs` for `Files` and `Tables` on the remote store (foundation hook); failure of that step does not fail the attach.
* The `fabric` scope exists in `TOKEN_SCOPES`; `fabric-internal` was added to `src/olcf/security/rbac.py` for this work package.
* Delta: the test runtime has no Delta jars. Production runtimes (`olcf-rt-2.0.0`) include them, but `scripts/verify_spark_delta.py` checks them on a machine with network access.

## 8. How to run the live checks

```
export OLCF_LIVE_FABRIC_TENANT=<tenant guid> OLCF_LIVE_FABRIC_CLIENT_ID=<app id> OLCF_LIVE_FABRIC_CLIENT_SECRET=<secret> \
       OLCF_LIVE_FABRIC_WORKSPACE=<workspace guid> OLCF_LIVE_FABRIC_NOTEBOOK=<notebook guid>
pytest tests/fabric/test_live.py -m live
```

Run `olcf.test_connection()` in a Fabric notebook as well; it records which context keys and `getDefinition` spellings your tenant supports (visible under the integration's metrics).
