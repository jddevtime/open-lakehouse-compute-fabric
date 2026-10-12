# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Observability metadata schema (migration stream ``observability``).

``metric_samples`` is the downsampled time-series store behind ``GET /v1/metrics/series`` (UI sparklines). Rows have a
``resolution`` in seconds: fresh samples are written at the sampling interval (15 s); a leader-only compaction job folds
raw rows older than an hour into 5-minute buckets and rows older than a day into 1-hour buckets; ``settings.retention.metric_days``
bounds everything. A series is identified by ``(name, labels_key)`` where ``labels_key`` is the canonical JSON of the labels.
"""

from __future__ import annotations

from sqlalchemy import JSON, Column, Float, Index, Integer, String, Table

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import TS, BigPK, metadata

metric_samples = Table(
    "metric_samples", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("ts", TS, nullable=False),                         # bucket start (UTC ISO)
    Column("resolution", Integer, nullable=False),            # seconds covered by this row (15 raw, 300, 3600)
    Column("name", String(128), nullable=False),
    Column("labels_key", String(512), nullable=False),
    Column("labels", JSON, nullable=False),
    Column("value", Float, nullable=False),                   # last (counters) or mean (gauges) of the bucket
    Column("vmin", Float),
    Column("vmax", Float),
    Column("n", Integer, nullable=False, default=1),
    Index("ix_metric_samples_series", "name", "labels_key", "ts"),
    Index("ix_metric_samples_ts", "resolution", "ts"),
)

OBS_TABLES = [metric_samples]

register(Migration("observability", 1, "initial observability schema", up=lambda c: create_tables(c, OBS_TABLES), down=lambda c: drop_tables(c, OBS_TABLES)))
