# Ports and networking

This page lists every connection OLCF makes or accepts. `olcf doctor` checks that the listening ports are free, `GET /v1/admin/ports` (`client.admin.ports()`) prints the effective values, and the **Administration, Health** page tests reachability of storage, identity providers and workers.

OLCF sends no telemetry and no usage data to anyone. The only outbound connections are those listed below, and each is either needed for a feature you use or configured by you (OTLP export, update feed, webhooks).

## Listening ports

Every Spark port is taken from a configured range, so a firewall or a NetworkPolicy can allow exactly those ranges. `GET /v1/admin/ports` (`client.admin.ports()` in the SDK; there is no dedicated CLI command) prints the effective values as plain text; with the defaults:

```text
API/UI              tcp/8470
Spark master RPC    tcp/7077-7092 (one per runtime family)
Spark master UI     tcp/18080-18095
Spark worker RPC    tcp/41200-41299 (one daemon per runtime family and node)
Spark worker UI     tcp/41300-41399
Driver UI range     tcp/4040-4056
Driver RPC range    tcp/41000-41099
BlockManager range  tcp/41100-41199
```

| Role | Setting | Default | Purpose | Who connects |
|---|---|---|---|---|
| API and web UI | `server.port` | tcp/8470 | REST API, web UI, `/mcp`, `/v1/workers/ws`, `/v1/docs`, Fabric submissions | users, CLI and SDK, worker agents, MCP clients, Fabric notebooks, load balancers |
| Spark master RPC | `compute.spark_master_port_base`, `compute.spark_master_port_span` | 7077, 16 ports (7077-7092) | one master per runtime family takes the first free port of the range | Spark worker daemons, drivers, executors |
| Spark master web UI | `compute.spark_master_ui_port_base`, `compute.spark_master_ui_port_span` | 18080, 16 ports (18080-18095) | Spark master UI | the control plane, which proxies it behind authentication; do not expose it |
| Spark worker daemon RPC | `compute.worker_port_range` | 41200-41299 | the master connects here to launch executors; one daemon per runtime family and node, the daemon keeps the port it was given for its lifetime | the master, other nodes of the cluster |
| Spark worker daemon web UI | `compute.worker_ui_port_range` | 41300-41399 | worker UI, pinned the same way | the master UI, which links to it |
| Driver web UI | `compute.driver_ui_port_range` | 4040-4056 | `spark.ui.port` is set to the first port of the range; Spark's own retry stays inside its default 16 neighbours | the control plane (Spark UI proxy) |
| Driver RPC | `compute.driver_port_range` | 41000-41099 | `spark.driver.port`, one port per Spark job on that node | executors |
| Block manager | `compute.block_manager_port_range` | 41100-41199 | `spark.blockManager.port`, one port per Spark job on that node | executors and driver |

Settings validation (`ComputeSettings`) rejects a range outside 1-65535, an empty master span and **overlapping ranges**, so a misconfiguration fails at start-up rather than as a mysterious connection error. The ranges are sizes, not promises of concurrency: a node can run at most as many concurrent Spark jobs as it has free driver and block-manager ports (100 each by default), and as many worker daemons as it has free ports in the two worker ranges.

OLCF does not set Spark RPC authentication or encryption (`spark.authenticate`, `spark.network.crypto.*`, `spark.ssl.*` are reserved so user code cannot change them, and nothing in OLCF turns them on). Treat the Spark ports as trusted-network traffic: allow them only between the cluster nodes and the control plane, never from outside. The chart's NetworkPolicy does exactly that; on other platforms use security groups or a host firewall. This has not been exercised on a real multi-machine network by the project, only on one host with several agents, and the chart's policy has been checked by rendering, not by a CNI that enforces it.

A job whose `engine` is `python`, and a Spark job running in local mode, open no cluster ports other than the driver and block manager ports of a Spark session.

### Helm: `spark.ports` and the network policy

In the Helm chart the ranges are one value, `spark.ports.<role>.{start,end}` (inclusive), with the roles `masterRpc`, `masterUi`, `workerRpc`, `workerUi`, `driverUi`, `driver` and `blockManager`. The same value feeds the ConfigMap (`OLCF_COMPUTE__SPARK_MASTER_PORT_BASE`/`_SPAN`, `OLCF_COMPUTE__SPARK_MASTER_UI_PORT_BASE`/`_SPAN`, `OLCF_COMPUTE__WORKER_PORT_RANGE`, `..._WORKER_UI_PORT_RANGE`, `..._DRIVER_UI_PORT_RANGE`, `..._DRIVER_PORT_RANGE`, `..._BLOCK_MANAGER_PORT_RANGE`) and `templates/networkpolicy.yaml`, so they cannot drift. The rules the chart renders when `networkPolicy.enabled` is true (the default):

| Policy | Direction | Allowed |
|---|---|---|
| control plane | ingress | the `http` port from everywhere (`networkPolicy.allowIngressFromAnywhere: true`), or, when `false`, only from this release's pods, `networkPolicy.ingressFrom` and `networkPolicy.monitoring.ingressFrom`; with an embedded worker, the driver and block-manager ranges from the worker pods (executors connect back to drivers hosted in the control-plane pod) |
| control plane | egress | everything when `networkPolicy.egressAllowAll: true` (default); otherwise DNS, the database (the bundled PostgreSQL by pod label, or `networkPolicy.egress.database`), HTTPS (`egress.https`), the Kubernetes API server when `kubernetesProvider.enabled` (`egress.apiServer`), **every** Spark range to the worker pods, then `networkPolicy.monitoring.egressTo` and `networkPolicy.egress.extra` verbatim |
| workers | ingress | every Spark range from the control plane; the peer ranges (everything except the driver UI) from other worker pods of the release; every range from `networkPolicy.workerIngressFrom` (for example an external driver) |
| workers | egress | everything when `egressAllowAll: true`; otherwise DNS, the control plane `http` port (agent connection, artifact upload), HTTPS, the peer ranges to other workers and `egress.extra` |

Peers are selected with this release's pod labels (`app.kubernetes.io/component` of `control-plane` or `worker`), not with namespaces, so other pods of the namespace get none of the Spark ranges. Notes:

* Changing a range is one edit: `--set spark.ports.driver.start=42000 --set spark.ports.driver.end=42099`. The values schema (`values.schema.json`) validates the shape; the control plane validates the overlap at start-up.
* Defaults of the restrictive branch are deliberately open to `0.0.0.0/0` for the database and HTTPS; narrow `egress.database.cidrs` and `egress.https.cidrs` for production.
* `kubernetesProvider.enabled: true` launches driver pods that need the API server: if it does not listen on 443, set `networkPolicy.egress.apiServer.ports` (default 6443) and `cidrs`.
* The chart has been rendered and its policy checked by tests (the first and last port of every range, moved and resized ranges). The policy takes effect only on a cluster whose CNI enforces NetworkPolicy, and `kubeconform` can validate the manifests against the Kubernetes schemas; run both checks with your own values.

## Outbound connections

| From | To | Port | When | Notes |
|---|---|---|---|---|
| worker agent | control plane `/v1/workers/ws` and `/v1/workers/enroll` | 8470 (or your public URL) | always | the agent only dials out; the control plane never connects to an agent |
| control plane | worker nodes: Spark master UI, worker UI, driver UI | the `compute.*` UI ranges (18080-18095, 41300-41399, 4040-4056) | when a user opens a Spark UI | proxied, authenticated |
| clients | control plane | 8470 | always | |
| control plane and workers | object storage: S3 or MinIO, ADLS Gen2, OneLake | 443 (or your endpoint) | when lakehouses use them | workers read and write data directly; the control plane reads for SQL, previews and the catalog |
| control plane | identity providers: OIDC discovery, JWKS, token endpoints; Entra ID | 443 | when SSO is configured | LDAP bridge: 389 or 636 |
| control plane | Microsoft Entra signing keys (`login.microsoftonline.com`) | 443 | when Fabric integration validates tokens | |
| control plane | Microsoft Entra (`login.microsoftonline.com`), Fabric API (`api.fabric.microsoft.com`), Power BI API (`api.powerbi.com`), OneLake DFS (`onelake.dfs.fabric.microsoft.com`) | 443 | only when an OLCF Intelligence `fabric` source exists and is scanned or tested | the scanner of [OLCF Intelligence](../guides/intelligence.md#connecting-microsoft-fabric-and-power-bi): metadata, definitions and Delta logs with a service principal. The hosts can be overridden per source (`config.network`: `authority_host`, `fabric_api`, `powerbi_api`, `onelake_dfs`, `proxy`, `ca_bundle`), but a private, loopback, link-local or metadata destination is refused unless the setting `intelligence.allow_private_endpoints` is on (the metadata address never is); this connector honours `HTTPS_PROXY` unless the source sets `trust_env` false. Never contacted without a Fabric source ([Fabric assumptions](../guides/fabric-assumptions.md#26-intelligence-discovery-the-scanner-and-the-fabric-and-power-bi-rest-apis)) |
| control plane | Kubernetes API; Fabric Livy | 443 | when those providers are configured | |
| control plane | webhooks, alert channels, `http` pipeline activity targets | as configured | when used | SSRF guard blocks private addresses unless allowed |
| control plane | SMTP relay | `alerts.smtp.port` (587) | when e-mail alerts are configured | |
| control plane | OTLP collector | as configured (4318) | when `observability.otlp.endpoint` is set | |
| control plane | update feed | https | only when a `feed_url` is set in the update configuration (`PUT /v1/admin/updates/config`) | off by default |
| control plane | Git hosts | 443 or 22 | when a workspace is connected to Git | needs a real `git` |
| worker | `pypi.org`, `files.pythonhosted.org`, `repo1.maven.org`, `api.adoptium.net` | 443 | during `olcf runtime install` and environment publishing | mirrors and wheelhouses replace these in restricted networks (`--index-url`, `--find-links`, `--bundle`, `environments.maven_repositories`) |

For an air-gapped site everything on the last row can be avoided with [offline bundles](../operations/runbook.md#air-gapped-sites).

## Proxies and private CAs

* Client tools honour `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` (httpx `trust_env`); the SDK takes a `proxy` argument.
* `--ca-bundle` / `OLCF_CA_BUNDLE` adds trusted CAs for the CLI and SDK. For outbound calls of the control plane use `tls.ca_bundle`.
* The built-in CA is `<data dir>/pki/ca.crt`; workers pin it at enrollment.
* Outbound HTTP made on behalf of users (pipeline `http` activity, webhooks, alert channels) never uses environment proxies, because a proxy would resolve the destination outside the SSRF check. The Intelligence Fabric connector is different: it talks to fixed Microsoft hosts, not to user-supplied URLs, and follows the proxy settings of the control-plane process unless the source says otherwise.

## Reverse proxy

Terminate TLS at a proxy or let the control plane do it (`tls.enabled`). A proxy must pass WebSocket upgrades for `/v1/workers/ws` and `/v1/sessions/{id}/ws`, must not buffer server-sent events on `/v1/events/stream`, and should allow large bodies for imports and uploads (`server.request_body_limit_mb` 256, `storage.max_upload_mb` 512). Examples that do this are in `deploy/caddy/Caddyfile` and `deploy/nginx/nginx.conf`. Set `server.public_url` to the external address.

Bind address: the server listens on 127.0.0.1 by default. Set `server.host` to expose it; do that together with TLS ([hardening](../operations/hardening.md)).
