# High availability

A single `olcf serve` process is a complete system, and for a team that is often enough. When the control plane must survive the loss of a machine, run several replicas against one PostgreSQL database. This page describes what the replicas share, what only the leader does, what happens on failure, and what is **not** made highly available.

## Model

* **All replicas are equal API servers.** They serve the REST API, the UI, the MCP endpoint and worker WebSocket connections at the same time. A write through one is visible through the others, and they accept each other's tokens.
* **One replica is the leader.** Singleton work runs only on the leader: the job scheduler, the pipeline dispatcher, schedule and trigger firing, alert evaluation, metric sampling and export, scheduled backups, the retention job, identity provider health tests and stale-job repair. The leader is chosen by a database lease (`control-plane`, time to live 15 seconds, renewed every 5). Others are followers and take over when the lease expires.
* **The database is the state.** Job states, pipeline runs, trigger claims and worker commands are rows updated with compare-and-set, so any replica can handle an agent's callback and a new leader resumes where the old one stopped ([components and data flow](../concepts/components-and-data-flow.md)). A worker connected to replica B is reached from the leader on A through the `worker_commands` outbox table.
* **Fencing.** A leader that cannot reach the database stops acting as leader when its own lease would expire, at the same moment the lease is free for everyone else, so two leaders never run singleton work together.

## Requirements

| Item | Requirement |
|---|---|
| Database | PostgreSQL (`db.url`, `OLCF_DB__URL`). SQLite cannot be shared between hosts |
| Keys | the same `OLCF_MASTER_KEY` and `OLCF_JWT_SIGNING_KEY` on every replica (otherwise one replica cannot decrypt secrets or verify the other's tokens) |
| Data directory | the built-in CA (`pki/`) and the signing key must be the same on all replicas: a shared `ReadWriteMany` volume, or your own certificates (`tls.cert_file`, `tls.key_file`) and keys supplied from a secret store |
| Address | `server.public_url` set to the external address (a load balancer or DNS name); workers connect there |
| Load balancer | any HTTP(S) balancer with WebSocket support, health check on `GET /v1/health/ready`. No sticky sessions are needed: interactive notebook sessions and event streams are served from the database on any replica |
| Initialisation | start one replica first so the schema is created and the data directory initialised, then add the others |

`olcf serve --workers N` starts N API processes on one host from the same command (use PostgreSQL: with SQLite the command only prints a warning that writers contend on one file; it turns the embedded worker off unless you pass `--embedded-worker`). Each process builds its own application, so they take part in the same lease-based leader election as replicas on separate hosts. Workers are not replicated; run as many as you need. They reconnect with backoff to `server.public_url`, so a replica failure is invisible to them apart from a short reconnect.

## What happens on failure

Measured by `tests/ops/test_ha.py`, which starts two real control-plane processes on one PostgreSQL 16 database (it needs `OLCF_TEST_POSTGRES_URL`; it was run for this guide and all five tests passed):

| Event | Behaviour |
|---|---|
| Normal operation | exactly one leader at all times; both replicas serve the same data |
| Leader killed (SIGKILL) | the survivor takes over within the lease TTL plus one renewal period (the test allows 28 s, normally about 15 to 20 s) and keeps serving; no metadata is lost |
| Leader stopped gracefully | the lease is handed over immediately |
| Replica restarted | rejoins as a follower; the audit hash chain stays intact |
| Both replicas down | after restart all metadata is present |
| Leader partitioned from the database | it stops acting as leader when its lease expires |

During the takeover window no new job is scheduled and no schedule fires, but running jobs are not affected (they run on workers), and a schedule time missed in the window is handled by the schedule's `catchup` policy ([pipelines](pipelines.md#schedules)). A job in flight when the controller changes is repaired by the new leader: it waits for the worker to report in, and fails the job after `compute.worker_lost_grace_seconds` (180 s) if the worker never returns.

## What is not highly available

* **PostgreSQL itself.** Use a managed service or your own replication and failover; OLCF only needs a reachable primary.
* **Lakehouse storage.** It is whatever your provider is.
* **Workers and Spark.** A worker that dies fails the jobs running on it. They are retried only as the retry policy and failure category allow ([compute model](../concepts/compute-model.md#failures-and-retries)). A Spark Standalone master runs on one worker node; losing it ends the jobs of that cluster.
* **The rate limiter** is per replica and in memory.

## Kubernetes

The Helm chart runs the control plane as a Deployment with `controlPlane.replicaCount`, a PodDisruptionBudget and readiness probes, requires an external or bundled PostgreSQL and a `ReadWriteMany` volume for more than one replica, and generates the shared keys once and keeps them across upgrades. See [Kubernetes](../operations/kubernetes.md).

## Checking it

`GET /v1/admin/health` (UI: Administration, Health) has a `leader` component that shows the holder, the term and whether the node you asked is the leader; `controlplane.leader_changed` events are written whenever leadership moves ([events reference](../reference/events.md)); `olcf status` shows workers and jobs. After a failover run `olcf audit verify` to confirm the audit chain.
