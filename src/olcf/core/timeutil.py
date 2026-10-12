# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""UTC timestamp helpers.

Timestamps are stored as ISO-8601 strings in UTC with microsecond precision and a
``Z`` suffix. They sort lexicographically, are portable across SQLite/PostgreSQL
and are trivially JSON serialisable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def utcnow() -> datetime:
    return datetime.now(UTC)


def now_iso() -> str:
    return to_iso(utcnow())


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def from_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso_in(seconds: float) -> str:
    return to_iso(utcnow() + timedelta(seconds=seconds))


def age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    return (utcnow() - from_iso(value)).total_seconds()
