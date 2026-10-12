<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 Justin Dodrill -->
# The OLCF REST API with curl

Everything the CLI, the SDK and the web UI do goes through this API. It is versioned (`/v1`), JSON, and described by an OpenAPI document:

```sh
curl -s $OLCF_URL/v1/openapi.json | head        # interactive docs at $OLCF_URL/v1/docs when the Swagger assets are bundled
```

All examples assume a local server (`olcf quickstart`). For https with a private CA add `--cacert ~/.olcf/pki/ca.crt`.

## Sign in without putting the password on a command line

```sh
export OLCF_URL=http://127.0.0.1:8470
read -rs OLCF_PASSWORD && export OLCF_PASSWORD                     # type it; nothing is echoed or stored in your shell history
TOKEN=$(printf '{"username":"admin","password":"%s"}' "$OLCF_PASSWORD" \
  | curl -fsS -X POST "$OLCF_URL/v1/identity/login" -H 'Content-Type: application/json' --data-binary @- \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
AUTH="Authorization: Bearer $TOKEN"
```

(`python3` here is only a JSON reader; use `jq -r .access_token` if you have it. On a Mac without the Xcode tools, use the installer's own interpreter
`~/.local/share/olcf/current/venv/bin/python` instead of `python3`.)

For scripts, create a long-lived scoped token instead of using the password: `olcf token create ci --scope read --scope write --days 30`
and send it as `Authorization: Bearer <token>`.

## Workspaces, lakehouses, tables

```sh
curl -fsS -X POST "$OLCF_URL/v1/workspaces" -H "$AUTH" -H 'Content-Type: application/json' -d '{"name":"demo"}'
LH=$(curl -fsS -X POST "$OLCF_URL/v1/workspaces/demo/lakehouses" -H "$AUTH" -H 'Content-Type: application/json' -d '{"name":"sales"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

printf 'region,amount\neu,10\nus,20\neu,5\n' \
  | curl -fsS -X PUT "$OLCF_URL/v1/lakehouses/$LH/files/content?path=Files/orders.csv&overwrite=true" -H "$AUTH" -H 'Content-Type: application/octet-stream' --data-binary @-

curl -fsS -X POST "$OLCF_URL/v1/lakehouses/$LH/tables" -H "$AUTH" -H 'Content-Type: application/json' -d '{"name":"orders","source":"Files/orders.csv"}'
curl -fsS "$OLCF_URL/v1/lakehouses/$LH/tables" -H "$AUTH"
```

## SQL (read-only, DuckDB; add `"engine":"spark"` for the cluster)

```sh
curl -fsS -X POST "$OLCF_URL/v1/sql/execute" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"sql":"select region, sum(amount) as total from sales.orders group by region order by region","lakehouse":"sales","workspace_id":"demo"}'
```

The answer has `columns`, `rows`, `row_count`, `truncated` and timing. Use `parameters` (`:name` placeholders) instead of pasting values into SQL.

## Jobs on the compute fabric

```sh
JOB=$(curl -fsS -X POST "$OLCF_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' -H 'Idempotency-Key: totals-run-1' \
  -d '{"kind":"sql","workspace_id":"demo","name":"totals","source":{"format":"sql","content":"select 1 as one"},"engine":"spark"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "$OLCF_URL/v1/jobs/$JOB" -H "$AUTH"                          # state: queued -> starting -> running -> succeeded
curl -fsS "$OLCF_URL/v1/jobs/$JOB/logs?offset=0&limit=65536" -H "$AUTH"
curl -fsS -X POST "$OLCF_URL/v1/jobs/$JOB/cancel" -H "$AUTH"
```

Repeating a `POST` with the same `Idempotency-Key` returns the original job instead of creating a second one, so a retry after a timeout is safe.
A job on a cluster without a worker or Spark runtime stays `queued`; `GET /v1/jobs/$JOB/timeline` shows its state transitions and the reason for each.

## Errors, paging, limits

* Errors are JSON: `{"error": {"code": "...", "message": "...", "remediation": "...", "correlation_id": "..."}}`; quote the `correlation_id` when you ask for help,
  it is in the server log and the audit trail. `401` = sign in again, `403` = your role does not allow it, `404` = not found or not visible to you,
  `409`/`412` = conflict, `422` = invalid input, `429` = rate limited (honour `Retry-After`).
* Lists take `limit` and `cursor`; follow `next_cursor` until it is `null`.
* Every request needs `Authorization: Bearer ...` except `/v1/health` and `/v1/identity/login`.

## From Python

`pip install olcf` gives you `olcf.sdk.Client`, which wraps all of this (retries, token refresh, paging, job waiting): see `examples/sdk_quickstart.py`.
