# ADR 0003: Workers connect outward over one WebSocket

Status: Accepted

## Context

Workers are ordinary machines: laptops, office PCs, cloud VMs, behind NAT and firewalls. The control plane must command them (run a job, start a kernel, start Spark daemons) and receive logs and heartbeats. Requiring an inbound port on every worker would make most machines unusable and widen the attack surface.

## Decision

Each worker agent opens **one outbound** WebSocket to `/v1/workers/ws` on the control plane and carries all traffic on it (protocol version `1.0`, frames validated strictly). The control plane never connects to a worker.

* Authentication: a bearer worker token, and, when `tls.require_client_cert_for_workers` is set, the client certificate's common name, checked against a revocation list ([ADR 0015](0015-built-in-ca-and-optional-mtls.md)).
* One connection per worker; a newer connection replaces an older one. A bounded send queue gives back-pressure, and an idle watchdog drops dead connections.
* `hello` is the reconcile point: on every (re)connect the agent reports its inventory, installed runtimes, running jobs and daemons, and the controller adopts or repairs state accordingly. Commands are requests with a reference and a timeout; results come back as `result{ref}`.
* Job logs are streamed in chunks straight to disk on the controller.
* Data does not go through the control plane: workers read and write lakehouse storage directly.

## Consequences

* Workers need only outbound access to the control plane. The machines of one Spark cluster, however, must reach each other on the Spark ports; that is inherent to Spark ([ports and networking](../reference/ports-and-networking.md)).
* Command delivery depends on the connection being up. A command to an offline worker fails with `WorkerOffline`; the scheduler treats a lost worker's jobs according to the retry rule.
* A load balancer in front of several replicas needs WebSocket support. A command for a worker held by another replica travels through the outbox ([ADR 0002](0002-database-as-state-and-leader-lease.md)).
* Worker tokens do not expire by default (`compute.worker_token_max_age_days = 0`; set it to give them a maximum age). They are revocable and are rotated together with the certificate over the authenticated channel (`olcf worker renew`, automatic inside the renewal window; the superseded token stays valid until the node reconnects with the new one, at most `compute.worker_token_grace_seconds`). The enrollment token that obtains one is single-use and short-lived.

## Alternatives considered

Controller connects to an agent port (rejected: inbound ports, NAT); polling over HTTP (rejected: latency and load for logs and interactive kernels); a message broker (rejected: another dependency).

## Verified by

`src/olcf/compute/hub.py`, `src/olcf/compute/protocol.py`, `src/olcf/worker/agent.py`, `tests/node/test_agent_protocol.py`, `tests/compute/`, `tests/integration/test_product_e2e.py`.
