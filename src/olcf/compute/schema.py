# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Compute metadata schema (migration stream ``compute``).

Design notes
------------
* The database is the single source of truth for node state, jobs, clusters and sessions so that any API replica
  can answer a request and a new leader can take over the scheduler without losing anything (spec section 27).
  Only socket-bound state (open WebSockets, in-flight request futures, log file handles) lives in process memory.
* ``jobs`` is indexed for >=100k history rows (spec section 34): list filters never need a table scan and the queue
  scan uses ``(state, queue, priority, queued_at)``.
* Large payloads never go into rows: job sources and logs live under ``<data_dir>/jobs/<job_id>/``.
* ``job_events`` stores every state transition (kind ``state``) and other notable run events (kind ``event``).
* ``worker_commands`` is the cross-replica outbox: the leader may need to command an agent that is connected to a
  different API replica; it inserts a row, the replica holding the socket delivers it and stores the result.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Float,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    inspect,
)
from sqlalchemy.engine import Connection

from olcf.db.migrate import Migration, add_column, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, BigPK, metadata

worker_pools = Table(
    "worker_pools", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False, unique=True),
    Column("description", Text, nullable=False, default=""),
    #: Reserved pools only accept jobs from ``workspace_ids`` (empty list = open to everyone).
    Column("reserved", Boolean, nullable=False, default=False),
    Column("workspace_ids", JSON, nullable=False, default=list),
    Column("labels", JSON, nullable=False, default=dict),  # labels applied to every member node (informational)
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

workers = Table(
    "workers", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("hostname", String(255)),
    Column("address", String(255)),  # address other nodes/the controller use to reach this node
    Column("state", String(16), nullable=False, default="pending"),  # WorkerState
    Column("scheduling", String(16), nullable=False, default="active"),  # SchedulingState
    Column("pool", String(64), nullable=False, default="default"),
    Column("labels", JSON, nullable=False, default=dict),
    Column("embedded", Boolean, nullable=False, default=False),
    Column("agent_version", String(64)),
    Column("protocol", String(16)),
    Column("os", String(128)),
    Column("arch", String(32)),
    Column("inventory", JSON, nullable=False, default=dict),  # normalised: cores, memory_mb, disk_gb, gpus, python...
    Column("metrics", JSON, nullable=False, default=dict),  # last heartbeat metrics
    Column("history", JSON, nullable=False, default=dict),  # ring buffers {cpu:[], mem:[], ts:[]} (<=120 samples)
    Column("runtimes", JSON, nullable=False, default=list),  # last reported [{id,state,verified,fingerprint}]
    Column("components", JSON, nullable=False, default=list),  # [{name,status,detail}]
    Column("spark_daemons", JSON, nullable=False, default=list),  # last reported daemons
    Column("active_jobs", JSON, nullable=False, default=list),  # last reported [{job_id,pid,state}]
    Column("notes", JSON, nullable=False, default=dict),  # orphans, clock skew, drain reason ...
    Column("principal_id", ID),
    Column("token_id", ID),
    Column("cert_serial", String(64)),
    Column("cert_expires_at", TS),  # not-after of the worker's machine certificate (NULL: no certificate / not known)
    Column("token_expires_at", TS),  # expiry of the current worker token (NULL: the token does not expire)
    Column("credentials", JSON),  # credential lifecycle bookkeeping: pending rotation (superseded token/serial + grace), operator grant, last renewal
    Column("connected", Boolean, nullable=False, default=False),
    Column("connected_node", String(128)),  # control-plane replica that holds the channel
    Column("enrolled_at", TS, nullable=False),
    Column("last_seen_at", TS),
    Column("last_connected_at", TS),
    Column("last_disconnected_at", TS),
    Column("revoked_at", TS),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("ix_workers_pool_state", "pool", "state"),
    Index("ix_workers_state", "state"),
)

worker_commands = Table(
    "worker_commands", metadata,
    Column("id", ID, primary_key=True),
    Column("worker_id", ID, nullable=False),
    Column("node_id", String(128), nullable=False),  # replica that must deliver the command
    Column("type", String(48), nullable=False),
    Column("payload", JSON, nullable=False, default=dict),
    Column("timeout_seconds", Float, nullable=False, default=30.0),
    Column("state", String(12), nullable=False, default="pending"),  # pending | sent | done | failed
    Column("result", JSON),
    Column("error", JSON),
    Column("created_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False),
    Index("ix_worker_commands_node_state", "node_id", "state"),
)

queues = Table(
    "queues", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False, unique=True),
    Column("description", Text, nullable=False, default=""),
    Column("max_concurrency", Integer),  # running+provisioning jobs allowed in this queue (None = unlimited)
    Column("max_cores", Integer),  # total granted cores allowed in this queue
    Column("max_memory_mb", Integer),
    Column("max_queued", Integer),  # submit is rejected beyond this depth
    Column("max_queue_wait_seconds", Integer),  # fail jobs that wait longer (None = never)
    #: {workspace_id: {"max_concurrency": n, "max_cores": n}}; key "*" is the default for every workspace
    Column("workspace_limits", JSON, nullable=False, default=dict),
    Column("fair_share_weights", JSON, nullable=False, default=dict),  # {workspace_id: weight}; missing = 1.0
    Column("allowed_pools", JSON, nullable=False, default=list),  # empty = any pool
    Column("default_priority", Integer, nullable=False, default=50),
    Column("paused", Boolean, nullable=False, default=False),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

providers = Table(
    "providers", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False, unique=True),
    Column("type", String(32), nullable=False),  # standalone | local | external | kubernetes | fabric | <entry point>
    Column("config", JSON, nullable=False, default=dict),  # non-secret configuration
    Column("secret_id", ID),  # opaque reference (tokens/kubeconfig/client secret)
    Column("enabled", Boolean, nullable=False, default=True),
    Column("is_default", Boolean, nullable=False, default=False),
    Column("last_discovery", JSON),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

runtimes = Table(
    "runtimes", metadata,
    Column("id", String(128), primary_key=True),  # manifest id, e.g. olcf-rt-2.0.0
    Column("family", String(64), nullable=False),
    Column("version", String(64), nullable=False),
    Column("spark", String(32)),
    Column("delta", String(32)),
    Column("iceberg", String(32)),
    Column("java", String(16)),
    Column("scala", String(16)),
    Column("python", String(16)),
    Column("manifest", JSON, nullable=False),
    Column("fingerprint", String(80)),  # sha256 of the manifest (nodes add the lock on install)
    Column("state", String(16), nullable=False, default="available"),  # available | retired
    Column("source", String(16), nullable=False, default="manifest"),  # manifest | api
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("ix_runtimes_family", "family", "version"),
)

runtime_installs = Table(
    "runtime_installs", metadata,
    Column("runtime_id", String(128), primary_key=True),
    Column("worker_id", ID, primary_key=True),
    Column("state", String(16), nullable=False),  # ready | installing | failed | retired | missing
    Column("verified", Boolean, nullable=False, default=False),
    Column("fingerprint", String(80)),
    Column("detail", Text),
    Column("updated_at", TS, nullable=False),
    Index("ix_runtime_installs_worker", "worker_id"),
)

runtime_bundles = Table(
    "runtime_bundles", metadata,
    Column("id", ID, primary_key=True),
    Column("runtime_id", String(128)),
    Column("filename", String(255), nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("size", BigInteger, nullable=False),
    Column("path", String(1024), nullable=False),  # below <data_dir>/artifacts/runtime-bundles
    Column("uploaded_by", ID),
    Column("created_at", TS, nullable=False),
)

clusters = Table(
    "clusters", metadata,
    Column("id", ID, primary_key=True),
    Column("provider", String(64), nullable=False),
    Column("runtime_id", String(128), nullable=False),
    Column("pool", String(64), nullable=False, default="default"),
    Column("family", String(64)),
    Column("state", String(16), nullable=False, default="stopped"),  # up | degraded | down | stopped | starting
    Column("master_worker_id", ID),
    Column("master_host", String(255)),
    Column("master_port", Integer),
    Column("ui_port", Integer),
    Column("master_url", String(512)),  # spark://host:port
    Column("ui_url", String(512)),  # internal master UI base (never returned to browsers)
    #: {worker_id: {"cores": n, "memory_mb": n, "master_url": url, "state": "starting|running|stopped", "port": n}}
    Column("spark_workers", JSON, nullable=False, default=dict),
    Column("last_status", JSON, nullable=False, default=dict),  # cached master /json (alive workers, apps)
    Column("last_status_at", TS),
    Column("detail", Text, nullable=False, default=""),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("provider", "runtime_id", "pool", name="uq_cluster_key"),
)

jobs = Table(
    "jobs", metadata,
    Column("id", ID, primary_key=True),
    Column("kind", String(16), nullable=False),
    Column("engine", String(8), nullable=False, default="spark"),
    Column("state", String(20), nullable=False),
    Column("state_reason", String(512)),
    Column("wait_reason", String(512)),  # why a queued job is still waiting (scheduler diagnostics)
    Column("workspace_id", ID, nullable=False),
    Column("name", String(255)),
    Column("item_id", ID),
    Column("submitted_by", ID),
    Column("origin_kind", String(16), nullable=False, default="api"),
    Column("origin", JSON, nullable=False, default=dict),
    Column("queue", String(64), nullable=False, default="default"),
    Column("priority", Integer, nullable=False, default=50),
    Column("attempt", Integer, nullable=False, default=1),
    Column("max_attempts", Integer, nullable=False, default=1),
    Column("runtime_id", String(128)),
    Column("runtime_fingerprint", String(80)),
    Column("source_hash", String(80)),
    Column("source_path", String(512)),  # relative to <data_dir>/jobs/<job_id>/
    Column("provider", String(64)),
    Column("cluster_id", ID),
    Column("application_id", String(128)),
    Column("driver_worker_id", ID),
    Column("provider_handle", JSON),  # provider-native handle for remote providers (k8s pod, Livy batch id)
    Column("resources_request", JSON, nullable=False, default=dict),
    Column("resources_granted", JSON, nullable=False, default=dict),
    Column("parameters", JSON, nullable=False, default=dict),
    Column("exit_value", JSON),
    Column("result", JSON, nullable=False, default=dict),
    Column("failure", JSON),
    Column("failure_category", String(24)),
    Column("parent_run_id", String(64)),
    Column("correlation_id", String(64)),
    Column("idempotency_key", String(191)),
    Column("request_hash", String(64)),
    Column("retry_of", ID),
    #: Non-sensitive submission document: policy, runtime request, lakehouse refs, env, secret *ids*, tags, compatibility.
    Column("spec", JSON, nullable=False, default=dict),
    Column("not_before", TS),  # retry back-off
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("queued_at", TS),
    Column("started_at", TS),
    Column("finished_at", TS),
    Column("timeout_at", TS),
    Column("cancel_requested_at", TS),
    Column("timed_out", Boolean, nullable=False, default=False),  # controller-side deadline fired; final state forced to timed_out
    Column("lost_since", TS),  # driver worker lost (grace period bookkeeping)
    UniqueConstraint("submitted_by", "idempotency_key", name="uq_jobs_idempotency"),
    Index("ix_jobs_queue_scan", "state", "queue", "priority", "queued_at"),
    Index("ix_jobs_ws_id", "workspace_id", "id"),
    Index("ix_jobs_item_id", "item_id", "id"),
    Index("ix_jobs_origin_id", "origin_kind", "id"),
    Index("ix_jobs_state_id", "state", "id"),
    Index("ix_jobs_kind_id", "kind", "id"),
    Index("ix_jobs_queue_id", "queue", "id"),
    Index("ix_jobs_submitter_id", "submitted_by", "id"),
    Index("ix_jobs_driver", "driver_worker_id", "state"),
    Index("ix_jobs_app", "application_id"),
    Index("ix_jobs_created", "created_at"),
    Index("ix_jobs_finished", "finished_at"),
    Index("ix_jobs_parent", "parent_run_id"),
)

job_events = Table(
    "job_events", metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("job_id", ID, nullable=False),
    Column("ts", TS, nullable=False),
    Column("kind", String(8), nullable=False, default="state"),  # state | event
    Column("from_state", String(20)),
    Column("to_state", String(20)),
    Column("reason", String(512)),
    Column("event", String(64)),  # for kind == event (e.g. spark_app_started, executor_hosts)
    Column("data", JSON, nullable=False, default=dict),
    Index("ix_job_events_job", "job_id", "seq"),
)

job_attempts = Table(
    "job_attempts", metadata,
    Column("id", ID, primary_key=True),
    Column("job_id", ID, nullable=False),
    Column("attempt", Integer, nullable=False),
    Column("worker_id", ID),
    Column("cluster_id", ID),
    Column("provider", String(64)),
    Column("state", String(20), nullable=False, default="provisioning"),
    Column("token_id", ID),  # job-scoped credential, revoked when the attempt ends
    Column("application_id", String(128)),
    Column("exit_code", Integer),
    Column("failure", JSON),
    Column("process_confirmed_gone", Boolean, nullable=False, default=False),
    Column("created_at", TS, nullable=False),
    Column("started_at", TS),
    Column("finished_at", TS),
    UniqueConstraint("job_id", "attempt", name="uq_job_attempt"),
)

sessions = Table(
    "sessions", metadata,
    Column("id", ID, primary_key=True),
    Column("workspace_id", ID, nullable=False),
    Column("kind", String(12), nullable=False, default="notebook"),  # notebook | sql
    Column("hidden", Boolean, nullable=False, default=False),
    Column("pool_key", String(255)),  # sql pool key: workspace|lakehouse|runtime
    Column("owner_id", ID),
    Column("notebook_id", ID),
    Column("engine", String(8), nullable=False, default="spark"),
    Column("state", String(12), nullable=False, default="starting"),
    Column("state_reason", String(512)),
    Column("runtime_id", String(128)),
    Column("provider", String(64)),
    Column("cluster_id", ID),
    Column("worker_id", ID),
    Column("resources", JSON, nullable=False, default=dict),
    Column("default_lakehouse", JSON),
    Column("spec", JSON),  # resolved session spec (env/lakehouses/runtime request); sensitive parts are built just-in-time at start
    Column("spark", JSON),  # {application_id, ui_url}
    Column("token_id", ID),
    Column("execution_count", Integer, nullable=False, default=0),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("last_activity_at", TS, nullable=False),
    Column("stopped_at", TS),
    Index("ix_sessions_ws_state", "workspace_id", "state"),
    Index("ix_sessions_worker", "worker_id", "state"),
    Index("ix_sessions_pool", "pool_key", "state"),
)

#: Output relay for interactive sessions (bounded: the newest ``compute.session_replay_events`` rows per session are kept).
#: Written by whichever replica holds the worker channel, tailed by whichever replica holds the browser WebSocket.
session_events = Table(
    "session_events", metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("session_id", ID, nullable=False),
    Column("ts", TS, nullable=False),
    Column("kind", String(12), nullable=False),  # output | reply | clear | spark | status
    Column("cell_id", String(64)),
    Column("data", JSON, nullable=False, default=dict),
    Index("ix_session_events_session", "session_id", "seq"),
)

spark_apps = Table(
    "spark_apps", metadata,
    Column("id", ID, primary_key=True),
    Column("application_id", String(128), nullable=False),
    Column("cluster_id", ID, nullable=False, default=""),  # '' for local[*] applications
    Column("workspace_id", ID),
    Column("job_id", ID),
    Column("session_id", ID),
    Column("name", String(255)),
    Column("state", String(24), nullable=False, default="RUNNING"),  # RUNNING | FINISHED | KILLED | FAILED | WAITING | UNKNOWN
    Column("cores", Integer),
    Column("memory_per_executor_mb", Integer),
    Column("executors", JSON, nullable=False, default=list),  # [{worker, cores, host}]
    Column("driver_worker_id", ID),
    Column("ui_url", String(512)),  # upstream UI base (internal; browsers use the authenticated proxy)
    Column("started_at", TS),
    Column("finished_at", TS),
    Column("last_seen_at", TS),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("application_id", "cluster_id", name="uq_spark_app"),
    Index("ix_spark_apps_job", "job_id"),
    Index("ix_spark_apps_ws", "workspace_id", "id"),
    Index("ix_spark_apps_cluster_state", "cluster_id", "state"),
)

COMPUTE_TABLES = [
    worker_pools, workers, worker_commands, queues, providers, runtimes, runtime_installs, runtime_bundles, clusters,
    jobs, job_events, job_attempts, sessions, session_events, spark_apps,
]

register(Migration("compute", 1, "initial compute schema", up=lambda c: create_tables(c, COMPUTE_TABLES),
                   down=lambda c: drop_tables(c, reversed(COMPUTE_TABLES))))


def _worker_credential_columns(conn: Connection) -> None:
    """compute 2: worker credential lifecycle columns (certificate / token expiry, rotation bookkeeping); a fresh database already has them from version 1."""
    have = {c["name"] for c in inspect(conn).get_columns("workers")}
    for name, ddl in (("cert_expires_at", "cert_expires_at VARCHAR(32)"), ("token_expires_at", "token_expires_at VARCHAR(32)"), ("credentials", "credentials JSON")):
        if name not in have:
            add_column(conn, "workers", ddl)


register(Migration("compute", 2, "worker credential lifecycle: certificate and token expiry, rotation state", up=_worker_credential_columns))
