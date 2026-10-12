# Using the Python SDK

`olcf.sdk` is a Python client for the REST API, with a synchronous `Client` and an `AsyncClient`. It is part of the `olcf` package, its only third-party dependency is `httpx`, and it is what the CLI itself uses. The complete method listing, generated from the code, is in the [SDK reference](../reference/sdk.md); the REST call behind every method is shown there so you can read it next to the [API reference](../reference/api.md).

Do not confuse it with the **Fabric SDK** (`olcf.fabric_sdk`, imported inside a Microsoft Fabric notebook to send work to your own cluster). That one is described in the [Fabric integration guide](fabric-integration.md) and the [Fabric SDK reference](../reference/fabric-sdk.md).

## Connecting

```python
from olcf.sdk import Client

# 1. explicit
c = Client("https://olcf.example.com", token="olcf_pat_...")

# 2. from the environment: OLCF_URL, OLCF_TOKEN (and OLCF_CA_BUNDLE for a private CA)
c = Client.from_env()

# 3. from what `olcf login` stored (the current context, or a named one)
c = Client.from_context()
c = Client.from_context("prod")

# 4. username and password; the client then keeps the access and refresh tokens
c = Client("http://127.0.0.1:8470")
c.login("admin", password)
```

`Client` is a context manager (`with Client.from_env() as c:`) and closes its connection pool on exit. Useful constructor arguments are `timeout` (default 60 s), `max_retries` (default 3; applies to safe requests, and to writes only when they carry an idempotency key), `verify` or `ca_bundle` for TLS, `proxy`, `headers`, and `auto_refresh` (default on: when the access token expires and a refresh token is present, the client refreshes once and retries; `from_context` writes the new tokens back to the context file).

`c.me()` returns `{"principal": {...}, "bindings": [...]}`. `c.wait_until_ready()` blocks until a freshly started server answers.

## Results and pagination

Methods return plain `dict` and `list` values decoded from JSON. List methods return a `Page`, a `list` with a `next_cursor` attribute. To walk every item use `paginate` (or the shortcut `c.jobs.iter(...)`):

```python
for job in c.paginate(c.jobs.list, state="failed", limit=100):
    print(job["id"], job["name"])
```

Workspaces and lakehouses can be addressed by id (`wks_...`, `itm_...`) or, where the reference says so, by unique name. Anything without a wrapper is reachable with `c.request("GET", "/jobs/stats")`; the path is relative to `/v1`.

## Examples

These were run against the sample workspace.

**SQL.**

```python
ws = c.workspaces.get("Sample Retail")
res = c.sql.execute(
    "select channel, count(*) as n from fact_sales group by 1 order by 2 desc",
    workspace_id=ws["id"], lakehouse="retail_lakehouse", max_rows=1000)
print([col["name"] for col in res["columns"]])   # ['channel', 'n']
print(res["rows"])                                # [['store', 41474], ['web', 27328]]
print(res["truncated"])                           # False
```

`engine="duckdb"` (the default) is read-only; `engine="spark"` runs on the cluster and is needed for writes. Parameters are bound, not interpolated: `c.sql.execute("select * from t where id = :id", parameters={"id": 7})`. `c.sql.export(...)` streams a full result as `csv`, `json`, `ndjson` or `parquet`.

**Items, lakehouses, tables.**

```python
for nb in c.items.list(ws["id"], type="notebook"):
    print(nb["name"])

lh = c.lakehouses.list_in_workspace(ws["id"])[0]
print(c.lakehouses.table_preview(lh["id"], "dim_store", rows=2)["columns"])
info = c.lakehouses.table(lh["id"], "dim_store")     # schema, protocol, history, statistics
c.lakehouses.upload_file(lh["id"], "Files/raw", "orders.csv", open("orders.csv", "rb").read())
```

Updating an item uses optimistic concurrency: read it, change it, send it back as `if_match`. A concurrent change raises `PreconditionFailedError` (412).

```python
item = c.items.get(item_id)
c.items.update(item_id, if_match=item, description="Nightly load")
```

**Jobs.**

```python
job = c.notebooks.run(notebook_id, parameters={"top_n": 5}, engine="python")
job = c.jobs.wait(job["id"], timeout=1800)             # polls; returns the final record
if job["state"] != "succeeded":
    print(c.jobs.timeline(job["id"]))

# or in one call; raises JobFailedError unless the job succeeded
job = c.jobs.submit_and_wait(kind="script", workspace_id=ws["id"], source={"format": "py", "content": "print(1)"}, timeout=600)

for chunk in c.jobs.logs(job["id"], follow=True):
    print(chunk, end="")

nb_bytes = c.jobs.output(job["id"])                  # the executed notebook
```

Submitting with `idempotency_key="..."` makes a retry safe. The shape of `source`, `runtime`, `resources` and `policy` is the `JobSubmission` schema in the [API reference](../reference/api.md) (`POST /v1/jobs`).

**Lineage, glossary, ownership and exposure.** `c.intelligence` wraps the asset graph and `c.governance` the glossary, domains, data products, owners, classifications, certification, change requests, policies and quality. Mutable governance objects carry an `etag` and take `if_match`; list calls return a `Page` and every paged list has an `iter_*` generator (`c.governance.iter_terms()`, `c.intelligence.iter_assets()`), also in the async client.

```python
asset = c.intelligence.list_assets(q="clean")[0]
impact = c.intelligence.impact(asset["id"])                         # downstream consumers with confidence
term = c.governance.create_term("Revenue", definition="Money earned from sales", synonyms=["turnover"])
c.governance.link_term(term["id"], asset_id=asset["id"])
c.governance.set_owners("asset", asset["id"], [{"role": "owner", "name": "Data Office", "email": "data@example.com"}])
c.governance.classify_asset(asset["id"], "PII")                     # inherited along the lineage, computed on demand
exposure = c.governance.classification_exposure("PII", types=["report"], min_confidence="inferred")
for e in exposure["exposures"]:
    print(e["asset"]["name"], "proven" if e["proven"] else "possible", e["confidence_floor"])

current = c.governance.get_term(term["id"])
c.governance.update_term(term["id"], current, definition="Money earned from sales, net of returns")   # 412 if it changed meanwhile
```

An asset you may not see raises `NotFoundError`, a missing permission `PermissionDeniedError`, a stale `if_match` `PreconditionFailedError`, a duplicate name or an illegal state change `ConflictError`, and a rejected request `BadRequestError`.

**Pipelines, secrets, administration.** Each area has a resource group: `c.pipelines`, `c.connections`, `c.environments`, `c.compute`, `c.runtimes`, `c.admin`, `c.identity`, `c.backups` and others (see the table in the reference). Secrets are write-only: `c.identity` and `c.connections` accept a secret value on create and update, and nothing returns it.

## Errors

Every error the SDK raises on purpose derives from `OlcfSdkError`. HTTP failures derive from `ApiError` and carry `status`, `code`, `message`, `remediation` and `correlation_id`.

```python
from olcf.sdk import NotFoundError, PermissionDeniedError, PreconditionFailedError, JobFailedError

try:
    c.workspaces.get("missing")
except NotFoundError as e:
    print(e.status, e.code)      # 404 not_found
```

The full class table is in the [SDK reference](../reference/sdk.md#errors) and the codes in the [errors reference](../reference/errors.md). `OlcfTimeoutError` from a `wait` helper means the work may still be running.

## Async

`AsyncClient` has the same resource groups with awaitable methods:

```python
import asyncio
from olcf.sdk import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8470", token="olcf_pat_...") as c:
        print((await c.me())["principal"]["name"])
        r = await c.sql.execute("select 42 as answer")
        print(r["rows"])                      # [[42]]
        async for chunk in c.jobs.logs(job_id, follow=True):
            print(chunk, end="")

asyncio.run(main())
```

`AsyncClient.from_env()` and `AsyncClient.from_context()` work like the synchronous constructors.

## Inside a job

Code running in a notebook or job on the cluster does not need a token in its source. The job specification handed to the runner carries an API URL and a short-lived `job`-scoped token, which is revoked when the job ends; the `notebookutils` and `mssparkutils` compatibility modules use it for `notebook.run`, `notebook.runMultiple` and `credentials.getSecret` (granted secrets only). See the [notebooks guide](notebooks.md).
