# ADR 0002: The database holds all state; singletons run on a lease holder

Status: Accepted

## Context

The control plane must survive restarts and run as several replicas for availability. Work that must happen once per cluster (the scheduler, the pipeline dispatcher, schedules, alerts, retention, scheduled backups) cannot run on every replica. Introducing a separate coordination service (etcd, Redis, ZooKeeper) would add an operational dependency to a product meant to run on a laptop.

## Decision

* All durable state is in the metadata database (SQLite for one process, PostgreSQL for several). Only socket-bound state, such as which WebSocket a worker is on, lives in process memory.
* Singleton work runs on the holder of a **database lease** named `control-plane` (`olcf.core.leader.LeaderElector`). The lease is acquired and renewed with a portable compare-and-set `UPDATE ... WHERE holder = me OR expires_at < now`, which behaves identically on SQLite and PostgreSQL. The default time to live is 15 seconds.
* The elector fences by clock: a replica that has not renewed successfully within the time to live stops considering itself leader at the moment the lease expires for everybody else, even if it cannot reach the database.
* Because a deposed leader may still have threads running, writes that end a unit of work are conditional on a token owned by the writer (for example the pipeline engine's `lease_token`), so the successor's records are never overwritten.
* Work for a worker connected to another replica goes through the `worker_commands` outbox table, so the leader can command any worker regardless of which replica holds its socket.

## Consequences

* No extra infrastructure. A single-process installation is simply a cluster of one that is always the leader.
* The lease `term` increments on every renewal, not only when leadership changes (observed on a running server). Do not use it to detect a change of leader; compare the holder.
* SQLite is for one process. Running several replicas on SQLite is not supported; `olcf serve --workers N` needs PostgreSQL.
* Failover time is bounded by the lease time to live (15 seconds by default; it is a constructor default, not a setting). Measured on PostgreSQL 16 by the project's HA test: after SIGKILL of the leader another node took over after 15.9 seconds; after a graceful stop, the hand-over took 3.8 seconds.
* The database is a single point of failure; its availability is the operator's job ([high availability](../guides/high-availability.md)).

## Alternatives considered

An external coordination service (rejected for operational weight); advisory locks in PostgreSQL only (rejected: would not work on SQLite); letting every replica run everything and relying on idempotence (rejected for the scheduler, which makes placement decisions).

## Verified by

`src/olcf/core/leader.py`, `src/olcf/compute/schema.py` (`worker_commands`), `tests/ops/test_ha.py` (needs PostgreSQL; it is the only test of failover and fencing).
