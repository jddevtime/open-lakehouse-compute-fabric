# Observability: health, metrics, logs, alerts and events

OLCF reports what it is doing through six channels. Nothing is sent anywhere unless you configure a destination.

| Channel | For | Where |
|---|---|---|
| Health | is each component working right now | `GET /v1/health` (liveness), `/v1/health/ready` (readiness) (probes); `GET /v1/admin/health` and the Health page; `olcf health`, `olcf doctor --server` |
| Metrics | trends and capacity | `GET /v1/metrics` (Prometheus); persisted series `GET /v1/metrics/series`; optional OTLP export |
| Traces | one request or run across components | OTLP export with W3C `traceparent` propagation |
| Logs | what happened in the server, the agents and each job | server and agent log files; job logs through `olcf job logs` |
| Events | something changed | `GET /v1/events`, a Server-Sent-Events stream, signed outbound webhooks |
| Alerts | a condition needs a person | rules, notification channels and a history under **Alerts and webhooks** |

The audit log, a separate security record, is described in the [security model](../concepts/security-model.md#audit).

## Health

`GET /v1/health` says the process is up (liveness); `GET /v1/health/ready` says it can serve (this is the load balancer probe; it also reports the node name and whether it is the leader):

```bash
curl -s https://olcf.example.com/v1/health/ready     # {"status":"ready","node":"vm-p29akg","leader":true}
olcf health                      # all components, as the Health page shows them
olcf doctor --server             # this machine plus the server's checks, with a remedy per problem
```

Readiness answers `503 {"status":"unavailable"}` (the same fixed body, no reason, because the probe is unauthenticated) when the database does not answer or when the `runtime-trust` check is `down`, that is, a production installation that trusts the public development runtime-signing key. A load balancer then stops routing to the node and a Kubernetes readiness probe takes the pod out of the Service's endpoints; liveness keeps answering `200`, so the process is **not** restarted and the reason stays readable with `olcf health` or `GET /v1/admin/health`. Fix the configuration (see [hardening, section 6](../operations/hardening.md#6-supply-chain)) and the node becomes ready again within the check's 30 second cache. Development installations and `olcf quickstart` are never affected.

Every component is `ok`, `degraded`, `down` or `unknown`. A quick check is cheap and runs on every refresh; a deep check performs a real end-to-end test (the **Test connectivity** buttons). The 29 checks of a default installation are listed in the [health checks reference](../reference/health-checks.md). `olcf diagnostics --out bundle.zip` writes a support bundle with versions, check results, a redacted configuration and recent logs, and no secrets, keys, tokens or data.

## Metrics

`GET /v1/metrics` is the Prometheus text exposition (format 0.0.4) of the replica you ask, plus gauges derived from the database. It needs a bearer token with the `events.read` permission: a service principal with the Viewer role and a token of scope `read` is enough. There is no anonymous access.

```bash
olcf user create prometheus --service --role Viewer
olcf token create prometheus-scrape --scope read --days 365 --for <principal-id of 'prometheus'>
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: olcf
    scheme: https
    metrics_path: /v1/metrics
    authorization: { credentials_file: /etc/prometheus/olcf.token }
    tls_config: { ca_file: /etc/prometheus/olcf-ca.crt }
    static_configs: [{ targets: ["olcf-1.example.com:8470", "olcf-2.example.com:8470"] }]
```

Scrape **every replica**: counters and histograms are per process, the database-derived gauges (`olcf_jobs`, `olcf_workers`, queue gauges) are the same on every replica, and `olcf_is_leader` tells which replica leads. A running installation exposes 47 metric families; the groups are:

| Group | Metrics (prefix `olcf_`) | Labels |
|---|---|---|
| API | `api_requests_total`, `api_errors_total`, `api_request_duration_seconds` (histogram), `api_inflight_requests`, `auth_failures_total` | method, route template, status or class |
| Jobs and queues | `jobs` (by state), `jobs_finished_total`, `job_runtime_seconds`, `queue_depth`, `queue_running`, `queue_paused`, `queue_oldest_wait_seconds`, `queue_saturation_ratio` | state, kind, queue |
| Workers and Spark | `workers` (by pool and state), `worker_cpu_percent`, `worker_memory_used_percent`, `worker_disk_used_percent`, `worker_last_seen_seconds`, `worker_network_*_bytes_per_second`, `spark_applications`, `spark_cores`, `spark_executors` | pool, worker |
| Pipelines | `pipeline_runs`, `pipeline_runs_created_total`, `pipeline_runs_total`, `pipeline_run_duration_seconds`, `pipeline_activity_runs_total`, `pipeline_activity_duration_seconds`, `pipeline_activities_inflight`, `pipeline_triggers` | pipeline, state, type, trigger |
| Control plane | `build_info`, `is_leader`, `uptime_seconds`, `process_start_time_seconds`, `db_ping_milliseconds`, `events_head_sequence`, `storage_failures_total`, `webhook_deliveries`, `webhook_oldest_due_lag_seconds`, `metrics_dropped_series_total` | node, version |
| Alerting and Fabric | `alerts_firing`, `alerts_fired_total`, `alert_evaluations_total`, `alert_notifications_total`, `fabric_submissions`, `fabric_submissions_last_hour` | severity, rule type, channel type |

These names and labels were read from a running server for this guide. Label cardinality is capped per metric; dropped combinations are counted in `olcf_metrics_dropped_series_total`.

Useful expressions: `olcf_queue_oldest_wait_seconds > 900` (jobs waiting for resources), `time() - olcf_worker_last_seen_seconds > 60`, `sum(rate(olcf_api_errors_total{class="5xx"}[5m]))`, `sum(olcf_is_leader) != 1` (no leader, or two).

### Built-in time series

The UI draws its sparklines from a series store in the database, sampled every `observability.sample_interval_seconds` (15) and kept `retention.metric_days` (14) days. It is queryable: `GET /v1/metrics/names`, and

```bash
curl -s -H "Authorization: Bearer $OLCF_TOKEN" "$OLCF_URL/v1/metrics/series?name=olcf_queue_depth&since=15m"
```

(`since` and `until` take an ISO time or a duration such as `15m`, `6h`, `7d`; `step` is in seconds, at least 15). This is meant for the interface, not for long-term storage: use Prometheus for that.

## OpenTelemetry

Set `observability.otlp.endpoint` to an OTLP/HTTP collector (for example `http://otel-collector:4318`) to export metrics and traces:

| Setting | Default | |
|---|---|---|
| `observability.otlp.metrics`, `.traces` | true, true | choose what to export |
| `observability.otlp.interval_seconds` | 30 | export period |
| `observability.otlp.trace_sample_rate` | 0.1 | share of API requests that start a trace; job and pipeline runs are always traced |
| `observability.otlp.headers_secret` | | id of a secret that holds request headers (`k=v,k2=v2`), for collectors that need an API key |

`POST /v1/metrics/otlp/test` (platform administrators) exports once and reports the outcome. A trace started by a caller continues through the control plane when the request carries a W3C `traceparent` header, and outbound calls of the `http` pipeline activity and of webhooks propagate it.

## Logs

* **Server.** `log_level` (`OLCF_LOG_LEVEL`) and `log_json` (`OLCF_LOG_JSON=true`, one JSON object per line). Files are in `<data dir>/logs`; `olcf serve --daemon` also writes `server.out`. Under systemd, Docker or Kubernetes read standard output. Every line carries correlation fields (`cid=` for the request, `trace=`, and `run=` or `job=` where they apply), so one grep follows a request through the API, the engine, a job and a webhook. Send `X-Correlation-ID` to choose the id; the response returns it.
* **Agents.** `<state dir>/logs/` on each worker (`olcf-worker run --json-logs --log-level DEBUG`). The Administration page (Diagnostics) creates a support bundle for the control plane (`POST /v1/admin/support-bundle`).
* **Jobs.** The output of every job is stored on the control plane and streamed while it runs: `olcf job logs JOB -f`, the job page in the UI, or the SDK. Driver and executor logs of Spark applications are reachable through the Spark UI proxy. Retention is `retention.log_days` (90).
* Secrets are redacted from logs, events and job output where the value is known to the platform (`olcf.core.redaction`); do not rely on this for secrets you print yourself.

## Events and webhooks

Every state change (jobs, pipeline runs, items, workers, runtimes, sessions, security and Fabric events, alerts) is an event with a type such as `job.failed` or `pipeline.run.succeeded`. The complete catalogue is the [events reference](../reference/events.md). Events are stored for `retention.event_days` (14).

* Read them: `GET /v1/events`, or follow `GET /v1/events/stream` (Server-Sent Events, resumable with `Last-Event-ID`).
* Receive them: **Alerts and webhooks, Webhooks**, or `POST /v1/webhooks`:

```python
c.request("POST", "/webhooks", json={
    "name": "ops-chat", "url": "https://hooks.example.com/olcf",
    "event_types": ["job.failed", "pipeline.run.failed", "alert.*"]})     # the signing secret is returned once
```

Deliveries are `POST`s of a JSON envelope `{id, type, created_at, workspace_id, subject, data, api_version}` with the headers `X-OLCF-Event`, `X-OLCF-Event-Id`, `X-OLCF-Delivery`, `X-OLCF-Timestamp` and `X-OLCF-Signature: sha256=<hex>`, where the hex is HMAC-SHA256 of `<timestamp>.<body>` with the subscription's secret (the same scheme as the pipeline webhook trigger). A receiver should recompute it and reject old timestamps. 2xx counts as delivered; network errors, 408, 425, 429 and 5xx are retried with exponential backoff and jitter (`Retry-After` is honoured); other 4xx are permanent, and 410 disables the subscription. After `max_attempts` (8) a delivery is `dead`: list the dead letters with `GET /v1/webhooks/{id}/deliveries?state=dead` and re-queue one with `POST /v1/webhooks/deliveries/{id}/redeliver`. Delivery is at least once and fan-out is idempotent across replicas. Target URLs pass the SSRF guard, redirects are not followed, and `POST /v1/webhooks/{id}/test` sends a synchronous test event.

## Alerts

An alert rule watches a condition, waits `for_seconds` for it to hold, then fires, notifies its channels and repeats until it is acknowledged, silenced or resolved. Evaluation runs on the leader every 30 seconds and is idempotent. Nine rule types exist (`GET /v1/alerts/rule-types` documents the parameters), and ten rules are installed by default, all enabled except the Fabric one and with no channel attached:

| Rule | Type | Default |
|---|---|---|
| Failed jobs | `failed_jobs` | 5 failures in 15 minutes, warning |
| Failed pipeline runs | `failed_pipelines` | 3 in 1 hour, warning |
| Queue saturation | `queue_saturation` | depth over 90% of the queue limit with at least 10 waiting, or oldest wait over 900 s, for 2 minutes, warning |
| Worker lost | `worker_loss` | no heartbeat for 60 s, draining workers ignored, critical |
| Disk pressure | `disk_pressure` | 90% used for 5 minutes, warning |
| Disk almost full | `disk_pressure` | 97% used for 1 minute, critical |
| Runtime mismatch | `runtime_mismatch` | workers report different fingerprints for the same runtime, for 5 minutes, warning |
| Credentials and certificates expiring | `auth_expiry` | API and worker tokens or the platform CA and server certificates expiring within 14 days, warning |
| Control-plane component down | `control_plane_health` | a health component `down` for 2 minutes, critical |
| Stale Fabric handshake | `fabric_handshake_stale` | disabled; no notebook handshake for 1 hour |

The default rules fire into the **Alerts** list but notify no one until you attach channels. Channels are `log` (to the server log), `webhook` (a signed JSON POST through the SSRF guard) and `email` (SMTP, configured with the `alerts.smtp.*` settings and a stored password):

```python
ch = c.request("POST", "/alerts/channels", json={"name": "oncall", "type": "webhook",
        "config": {"url": "https://hooks.example.com/olcf-alerts"}, "secret": "signing secret"})
c.request("POST", f"/alerts/channels/{ch['id']}/test")            # {"ok": true, "detail": ...}
rule = [r for r in c.request("GET", "/alerts/rules")["items"] if r["type"] == "worker_loss"][0]
c.request("PATCH", f"/alerts/rules/{rule['id']}", json={"channels": [ch["id"]], "repeat_seconds": 3600})
```

(In this repository's tests a `log` channel was created and tested on the sample installation: `{"ok": true, "detail": "logged"}`.) Alerts can be acknowledged (stops repeats) and silenced (`minutes`, up to 90 days) per alert or per rule; transitions also publish `alert.firing`, `alert.resolved` and `alert.acknowledged` events, so a webhook subscription can carry them as well. Resolved alerts are kept `alerts.history_days` (90).

## A minimal production setup

1. Probe `/v1/health/ready` from the load balancer; scrape `/v1/metrics` on every replica; alert on `sum(olcf_is_leader) != 1`.
2. Attach an email or webhook channel to the Worker lost, Disk almost full, Control-plane component down and Credentials expiring rules.
3. Send logs to your collector (`OLCF_LOG_JSON=true`), and metrics and traces through OTLP if you run a collector.
4. Subscribe a webhook to `job.failed` and `pipeline.run.failed` for the teams that own the workloads.
5. Run `olcf audit verify` on a schedule and alert on failure.
