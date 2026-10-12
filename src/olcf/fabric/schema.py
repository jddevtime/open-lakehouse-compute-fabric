# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fabric adapter tables (migration stream ``fabric``).

* ``fabric_config`` - the single integration configuration document (allowed tenants/audiences, workspace mapping, broker connection, default policy) plus
  the id of the secret holding the capsule-key derivation material. One row, ``id = 'default'``.
* ``fabric_nonces`` - single-use challenge nonces (replay protection). A nonce is bound to the principal that asked for it and to the submission id issued with it.
* ``fabric_submissions`` - one row per capsule received (accepted *or* rejected): who sent what from which Fabric notebook, what the analyzer found and what became
  of the job. Parameter *values* are stored redacted; the capsule itself is never stored here (the job holds the source).
* ``fabric_handshakes`` - what the SDK's ``test_connection()`` reported: the proof that a Fabric notebook reached this control plane.
"""

from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Boolean, Column, Float, Index, String, Table, Text, UniqueConstraint

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

fabric_config = Table(
    "fabric_config", metadata,
    Column("id", ID, primary_key=True),
    Column("config", JSON, nullable=False, default=dict),
    Column("signing_secret_id", ID),
    Column("updated_at", TS, nullable=False),
    Column("updated_by", ID),
)

fabric_nonces = Table(
    "fabric_nonces", metadata,
    Column("nonce", String(128), primary_key=True),
    Column("principal_id", ID, nullable=False),
    Column("submission_id", ID, nullable=False),
    Column("issued_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False),
    Column("used_at", TS),
    Column("capsule_sha256", String(80)),
    Index("ix_fabric_nonces_exp", "expires_at"),
    Index("ix_fabric_nonces_sub", "submission_id"),
)

fabric_submissions = Table(
    "fabric_submissions", metadata,
    Column("id", ID, primary_key=True),                               # == capsule.submissionId (sub_<ULID>)
    Column("received_at", TS, nullable=False),
    Column("principal_id", ID, nullable=False),
    Column("idempotency_key", String(200), nullable=False),
    Column("tenant_id", String(64)),
    Column("fabric_workspace_id", String(64)),
    Column("fabric_workspace_name", String(256)),
    Column("notebook_id", String(64)),
    Column("notebook_name", String(256)),
    Column("default_lakehouse", JSON),                                # {id, name, workspace_id}
    Column("run_id", String(128)),                                    # Fabric currentRunId
    Column("parent_run_id", String(128)),
    Column("is_for_pipeline", Boolean, nullable=False, default=False),
    Column("caller", JSON, nullable=False, default=dict),             # {principal_id, name, kind, source, claims: {tid, oid, appid, ...}}
    Column("sdk_version", String(40)),
    Column("adapter_version", String(80)),
    Column("source_mode", String(8), nullable=False, default="inline"),
    Column("capsule_bytes", BigInteger, nullable=False, default=0),
    Column("capsule_sha256", String(80)),
    Column("parameters", JSON, nullable=False, default=dict),         # {name: redacted value}
    Column("compatibility", JSON),                                    # full analyzer report (redacted)
    Column("compat_summary", JSON),                                   # {counts, blockers, shims, blocked}
    Column("olcf_workspace_id", ID),
    Column("job_id", ID),
    Column("state", String(16), nullable=False, default="received"),  # received | validated | rejected | queued | running | succeeded | failed | cancelled | timed_out
    Column("failure_category", String(32)),
    Column("error", Text),
    Column("round_trip_ms", Float),                                   # received -> terminal state
    Column("secret_findings", JSON),                                  # [{path, kind, severity}] (never values)
    Column("correlation_id", String(200)),
    Column("finished_at", TS),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("principal_id", "idempotency_key", name="uq_fabric_sub_idem"),
    Index("ix_fabric_sub_received", "received_at"),
    Index("ix_fabric_sub_job", "job_id"),
    Index("ix_fabric_sub_state", "state"),
    Index("ix_fabric_sub_notebook", "notebook_id"),
)

fabric_handshakes = Table(
    "fabric_handshakes", metadata,
    Column("id", ID, primary_key=True),
    Column("received_at", TS, nullable=False),
    Column("principal_id", ID, nullable=False),
    Column("ok", Boolean, nullable=False, default=True),
    Column("tenant_id", String(64)),
    Column("fabric_workspace_id", String(64)),
    Column("fabric_workspace_name", String(256)),
    Column("notebook_id", String(64)),
    Column("notebook_name", String(256)),
    Column("user", JSON),                                             # {name, id} as Fabric reported it (informational)
    Column("sdk_version", String(40)),
    Column("latency_ms", Float),
    Column("checks", JSON),                                           # the SDK's checklist
    Column("context_keys", JSON),                                     # which Fabric context keys were seen
    Column("server_version", String(40)),
    Column("source_ip", String(64)),
    Index("ix_fabric_hs_received", "received_at"),
    Index("ix_fabric_hs_principal", "principal_id"),
)

FABRIC_TABLES = [fabric_config, fabric_nonces, fabric_submissions, fabric_handshakes]

register(Migration("fabric", 1, "initial Fabric adapter schema", up=lambda c: create_tables(c, FABRIC_TABLES),
                   down=lambda c: drop_tables(c, reversed(FABRIC_TABLES))))

__all__ = ["FABRIC_TABLES", "fabric_config", "fabric_handshakes", "fabric_nonces", "fabric_submissions"]
