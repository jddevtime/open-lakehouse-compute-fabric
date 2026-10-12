# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Orchestration metadata schema (migration stream ``pipelines``).

Design notes
------------
* The database is the single source of truth for runs, so any API replica can accept ``POST .../runs`` (it only inserts a
  ``queued`` row) and a new leader can resume driving in-flight runs after a failover (``engine.PipelineEngine.recover``).
* ``activity_runs`` rows are created lazily, when an activity's dependencies are terminal; the unique ``(run_id, scope_key)``
  makes row creation idempotent, so a crash between "decide" and "write" can never create a duplicate execution.
* Every write that finishes an activity is a compare-and-set on ``lease_token`` - a deposed leader whose thread is still
  running cannot overwrite the outcome that the new leader recorded.
* ``trigger_runs`` carries ``UNIQUE(trigger_id, fire_key)``: that unique index *is* the exactly-once claim of a schedule fire
  time (or webhook delivery / file batch / event) across HA replicas.
* ``webhook_deliveries`` carries ``UNIQUE(webhook_id, event_id)`` so enqueueing an event is idempotent as well.
* ``copy_manifest`` is the resumable manifest of the ``copy`` activity (one row per file, keyed by a manifest key that
  survives retries and resume-from-failure re-runs).
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
)

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

pipeline_runs = Table(
    "pipeline_runs", metadata,
    Column("id", ID, primary_key=True),                              # run_<ULID>
    Column("workspace_id", ID, nullable=False),
    Column("pipeline_id", ID, nullable=False),                       # items.id
    Column("pipeline_name", String(128), nullable=False),
    Column("pipeline_version", Integer, nullable=False, default=1),
    Column("definition", JSON, nullable=False),                      # immutable snapshot the run executes
    Column("def_hash", String(64), nullable=False),
    Column("parameters", JSON, nullable=False, default=dict),
    Column("variables", JSON, nullable=False, default=dict),
    Column("state", String(24), nullable=False),                     # queued|running|cancel_requested|succeeded|failed|cancelled|skipped
    Column("state_reason", String(512)),
    Column("trigger", JSON, nullable=False, default=dict),
    Column("trigger_type", String(24), nullable=False, default="manual"),
    Column("principal_id", ID),                                      # whose permissions activities run with
    Column("parent_run_id", ID),
    Column("parent_activity_id", ID),
    Column("root_run_id", ID),
    Column("depth", Integer, nullable=False, default=0),
    Column("resumed_from", ID),
    Column("resume_options", JSON, nullable=False, default=dict),
    Column("group_id", ID),                                          # all re-runs of one logical run share the group
    Column("idempotency_key", String(191), unique=True),
    Column("queue_policy", String(16), nullable=False, default="queue"),
    Column("concurrency", Integer, nullable=False, default=0),
    Column("outputs", JSON),
    Column("error", JSON),
    Column("correlation_id", String(64)),
    Column("created_at", TS, nullable=False),
    Column("started_at", TS),
    Column("finished_at", TS),
    Column("updated_at", TS, nullable=False),
    Column("deadline_at", TS),
    Column("cancel_requested_at", TS),
    Column("cancel_reason", String(512)),
    Index("ix_pipeline_runs_ws_created", "workspace_id", "created_at"),
    Index("ix_pipeline_runs_pipeline_created", "pipeline_id", "created_at"),
    Index("ix_pipeline_runs_state", "state"),
    Index("ix_pipeline_runs_parent", "parent_run_id"),
    Index("ix_pipeline_runs_group", "group_id"),
)

activity_runs = Table(
    "activity_runs", metadata,
    Column("id", ID, primary_key=True),                              # act_<ULID>
    Column("run_id", ID, nullable=False),
    Column("scope_key", String(512), nullable=False),                # "<parent id|->|<iteration|->|<name>"
    Column("parent_id", ID),                                         # container activity row (if/for_each/switch/until)
    Column("iteration", Integer),
    Column("name", String(128), nullable=False),
    Column("type", String(48), nullable=False),
    Column("state", String(16), nullable=False),                     # ready|running|retry_wait|succeeded|failed|skipped|cancelled
    Column("attempt", Integer, nullable=False, default=0),
    Column("max_attempts", Integer, nullable=False, default=1),
    Column("def_hash", String(64), nullable=False),
    Column("input", JSON),                                           # redacted evaluated typeProperties
    Column("output", JSON),
    Column("error", JSON),
    Column("state_data", JSON, nullable=False, default=dict),        # activity-private durable state (job ids, deadlines, ...)
    Column("signal", JSON),                                          # external signal delivered to the activity (webhook callback body)
    Column("attempts", JSON, nullable=False, default=list),          # [{attempt, state, started_at, finished_at, error}]
    Column("job_ids", JSON, nullable=False, default=list),
    Column("reused_from", ID),
    Column("lease_token", String(64)),
    Column("lease_expires_at", TS),
    Column("next_attempt_at", TS),
    Column("timeout_at", TS),
    Column("cancel_requested", Boolean, nullable=False, default=False),
    Column("created_at", TS, nullable=False),
    Column("started_at", TS),
    Column("finished_at", TS),
    Column("updated_at", TS, nullable=False),
    Column("duration_ms", BigInteger),
    UniqueConstraint("run_id", "scope_key", name="uq_activity_scope"),
    Index("ix_activity_runs_run", "run_id"),
    Index("ix_activity_runs_state", "state"),
    Index("ix_activity_runs_parent", "parent_id"),
)

triggers = Table(
    "triggers", metadata,
    Column("id", ID, primary_key=True),                              # trg_<ULID>
    Column("workspace_id", ID, nullable=False),
    Column("name", String(128), nullable=False),
    Column("type", String(24), nullable=False),                      # schedule|webhook|storage_event|dependency
    Column("target_type", String(32), nullable=False, default="pipeline"),   # pipeline|notebook|spark_job_definition
    Column("target_id", ID, nullable=False),
    Column("item_id", ID),                                           # backing `schedule` item (schedule triggers only)
    Column("parameters", JSON, nullable=False, default=dict),        # literal values or expressions over @trigger()
    Column("config", JSON, nullable=False, default=dict),
    Column("state", JSON, nullable=False, default=dict),             # cursors: last_fire, seen files, event seq, ...
    Column("enabled", Boolean, nullable=False, default=True),
    Column("paused_until", TS),
    Column("secret_id", ID),                                         # HMAC key of a webhook trigger
    Column("token_hash", String(128)),                               # sha256 of the bearer token of a webhook trigger
    Column("owner_id", ID),
    Column("disabled_reason", String(512)),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
    Column("last_fired_at", TS),
    Column("last_status", String(24)),
    Index("ix_triggers_ws", "workspace_id"),
    Index("ix_triggers_type", "type", "enabled"),
    Index("ix_triggers_item", "item_id"),
    Index("ix_triggers_target", "target_id"),
)

trigger_runs = Table(
    "trigger_runs", metadata,
    Column("id", ID, primary_key=True),                              # trr_<ULID>
    Column("trigger_id", ID, nullable=False),
    Column("fire_key", String(191), nullable=False),                 # schedule: ISO fire time; webhook: delivery id; storage: batch hash; dependency: event id
    Column("fire_time", TS, nullable=False),                         # the (scheduled) time this firing is for
    Column("status", String(16), nullable=False),                    # claimed|started|skipped|failed
    Column("reason", String(512)),
    Column("run_id", ID),
    Column("job_id", ID),
    Column("detail", JSON, nullable=False, default=dict),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("trigger_id", "fire_key", name="uq_trigger_fire"),
    Index("ix_trigger_runs_trigger", "trigger_id", "fire_time"),
)

copy_manifest = Table(
    "copy_manifest", metadata,
    Column("manifest_key", String(64), nullable=False, primary_key=True),
    Column("rel_path", String(1024), nullable=False, primary_key=True),
    Column("state", String(12), nullable=False),                     # pending|done|failed
    Column("size", BigInteger),
    Column("sha256", String(64)),
    Column("rows", BigInteger),
    Column("source_etag", String(128)),
    Column("attempts", Integer, nullable=False, default=0),
    Column("error", String(1000)),
    Column("updated_at", TS, nullable=False),
)

webhooks = Table(
    "webhooks", metadata,
    Column("id", ID, primary_key=True),                              # hok_<ULID>
    Column("workspace_id", ID),                                      # NULL => organisation-wide subscription
    Column("name", String(128), nullable=False),
    Column("url", String(2048), nullable=False),
    Column("event_types", JSON, nullable=False, default=list),       # patterns: "job.*", "pipeline.run.failed", "*"
    Column("secret_id", ID),                                         # signing key (secrets store)
    Column("headers", JSON, nullable=False, default=dict),           # extra non-secret request headers
    Column("enabled", Boolean, nullable=False, default=True),
    Column("max_attempts", Integer, nullable=False, default=8),
    Column("timeout_seconds", Float, nullable=False, default=10.0),
    Column("description", Text, nullable=False, default=""),
    Column("owner_id", ID),
    Column("consecutive_failures", Integer, nullable=False, default=0),
    Column("last_delivery_at", TS),
    Column("last_status", String(16)),
    Column("disabled_reason", String(512)),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
    Index("ix_webhooks_ws", "workspace_id"),
)

webhook_deliveries = Table(
    "webhook_deliveries", metadata,
    Column("id", ID, primary_key=True),                              # whd_<ULID>
    Column("webhook_id", ID, nullable=False),
    Column("event_id", ID, nullable=False),
    Column("event_seq", BigInteger),
    Column("event_type", String(96), nullable=False),
    Column("workspace_id", ID),
    Column("payload", JSON, nullable=False),                         # the exact envelope that is signed and POSTed
    Column("state", String(12), nullable=False),                     # pending|retrying|delivering|succeeded|dead
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", TS),
    Column("last_status_code", Integer),
    Column("last_error", String(1000)),
    Column("last_attempt_at", TS),
    Column("response_ms", Integer),
    Column("attempt_log", JSON, nullable=False, default=list),
    Column("is_test", Boolean, nullable=False, default=False),
    Column("created_at", TS, nullable=False),
    Column("delivered_at", TS),
    UniqueConstraint("webhook_id", "event_id", name="uq_delivery_event"),
    Index("ix_deliveries_due", "state", "next_attempt_at"),
    Index("ix_deliveries_webhook", "webhook_id", "created_at"),
)

alert_channels = Table(
    "alert_channels", metadata,
    Column("id", ID, primary_key=True),                              # chn_<ULID>
    Column("workspace_id", ID),
    Column("name", String(128), nullable=False),
    Column("type", String(16), nullable=False),                      # webhook|email|log
    Column("config", JSON, nullable=False, default=dict),            # url / recipients / ... (never secret material)
    Column("secret_id", ID),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("owner_id", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
)

alert_rules = Table(
    "alert_rules", metadata,
    Column("id", ID, primary_key=True),                              # arl_<ULID>
    Column("workspace_id", ID),                                      # NULL => platform rule
    Column("name", String(128), nullable=False),
    Column("description", Text, nullable=False, default=""),
    Column("type", String(32), nullable=False),
    Column("severity", String(12), nullable=False, default="warning"),
    Column("params", JSON, nullable=False, default=dict),
    Column("for_seconds", Integer, nullable=False, default=0),
    Column("repeat_seconds", Integer, nullable=False, default=3600),
    Column("channels", JSON, nullable=False, default=list),
    Column("labels", JSON, nullable=False, default=dict),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("builtin_key", String(64), unique=True),                  # set for shipped default rules
    Column("silenced_until", TS),
    Column("owner_id", ID),
    Column("last_evaluated_at", TS),
    Column("last_error", String(500)),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
    Index("ix_alert_rules_ws", "workspace_id"),
)

alerts = Table(
    "alerts", metadata,
    Column("id", ID, primary_key=True),                              # alr_<ULID>; one row per firing episode (the alert history)
    Column("rule_id", ID, nullable=False),
    Column("instance_key", String(191), nullable=False),            # dedupe key within the rule (e.g. "queue=default")
    Column("workspace_id", ID),
    Column("severity", String(12), nullable=False),
    Column("state", String(12), nullable=False),                     # pending|firing|resolved
    Column("summary", String(500), nullable=False),
    Column("value", Float),
    Column("labels", JSON, nullable=False, default=dict),
    Column("details", JSON, nullable=False, default=dict),
    Column("started_at", TS, nullable=False),                        # condition first true
    Column("fired_at", TS),                                          # `for` elapsed
    Column("resolved_at", TS),
    Column("last_seen_at", TS, nullable=False),
    Column("last_notified_at", TS),
    Column("notify_count", Integer, nullable=False, default=0),
    Column("acknowledged_at", TS),
    Column("acknowledged_by", ID),
    Column("silenced_until", TS),
    Column("fingerprint", String(64), nullable=False),
    Index("ix_alerts_rule_instance", "rule_id", "instance_key", "state"),
    Index("ix_alerts_state", "state", "severity"),
    Index("ix_alerts_started", "started_at"),
)

PIPELINE_TABLES = [pipeline_runs, activity_runs, triggers, trigger_runs, copy_manifest, webhooks, webhook_deliveries, alert_channels, alert_rules, alerts]

register(Migration("pipelines", 1, "initial orchestration schema", up=lambda c: create_tables(c, PIPELINE_TABLES), down=lambda c: drop_tables(c, reversed(PIPELINE_TABLES))))
