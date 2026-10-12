# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Metrics aggregation and the readiness checklist, computed from rows in ``fabric_submissions`` / ``fabric_handshakes``."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import insert

from olcf.fabric import schema as S
from olcf.fabric.metrics import compute_metrics, counters_summary, percentile, readiness
from tests.fabric.fabric_stack import FabricStack

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FabricStack]:
    st = FabricStack(tmp_path_factory.mktemp("fabric-metrics")).start()
    try:
        yield st
    finally:
        st.stop()


def sub(ago_s: int, state: str, **kw: Any) -> dict[str, Any]:
    base = {"id": "sub_" + uuid.uuid4().hex, "received_at": (NOW - timedelta(seconds=ago_s)).isoformat(), "principal_id": "p1", "idempotency_key": uuid.uuid4().hex, "tenant_id": "t1",
            "fabric_workspace_id": "ws1", "fabric_workspace_name": "Sales", "notebook_id": "nb1", "notebook_name": "Load", "is_for_pipeline": False, "caller": {}, "sdk_version": "0.1.0",
            "source_mode": "inline", "capsule_bytes": 1000, "updated_at": NOW.isoformat(), "parameters": {}, "state": state}
    base.update(kw)
    return base


def test_percentile() -> None:
    assert percentile([], 0.5) is None and percentile([5.0], 0.99) == 5.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.5 and percentile([1.0, 2.0, 3.0, 4.0], 1.0) == 4.0


def test_compute_metrics_windows_counters_and_rankings(stack: FabricStack) -> None:
    rows = [sub(60, "succeeded", round_trip_ms=1000.0, is_for_pipeline=True, compat_summary={"counts": {"native-portable": 3, "warning": 1}}, parameters={"a": 1, "b": 2}),
            sub(120, "succeeded", round_trip_ms=3000.0, source_mode="pull", notebook_id="nb2", notebook_name="Other", default_lakehouse={"name": "lh"}),
            sub(300, "failed", failure_category="user-code", round_trip_ms=500.0),
            sub(400, "rejected", error="[capsule_replay] used", failure_category="capsule_replay"),
            sub(900, "running"),
            sub(3 * 3600, "succeeded", round_trip_ms=2000.0),                       # inside 24h, outside 1h
            sub(3 * 86400, "cancelled")]                                              # inside 7d only
    hs = [{"id": "hs_" + uuid.uuid4().hex, "received_at": (NOW - timedelta(seconds=30)).isoformat(), "principal_id": "p1", "ok": True, "tenant_id": "t1", "fabric_workspace_id": "ws1",
           "sdk_version": "0.1.0", "latency_ms": 12.0, "context_keys": ["currentNotebookName", "currentWorkspaceId"]}]
    with stack.ctx.db.begin() as conn:
        for r in rows:
            conn.execute(insert(S.fabric_submissions).values(**r))
        for h in hs:
            conn.execute(insert(S.fabric_handshakes).values(**h))
    one = compute_metrics(stack.ctx, "1h", now=NOW)
    c = one["counters"]
    assert (c["received"], c["rejected"], c["accepted"], c["succeeded"], c["failed"], c["in_flight"]) == (5, 1, 4, 2, 1, 1)
    assert c["success_rate"] == round(2 / 3, 4) and one["windows"]["7d"]["received"] == 7 and one["windows"]["24h"]["received"] == 6
    assert one["latency_ms"]["n"] == 3 and one["latency_ms"]["p50"] == 1000.0 and one["latency_ms"]["mean"] == 1500.0
    assert one["errors_by_category"] == {"user-code": 1, "capsule_replay": 1} and one["rejections_by_code"] == {"capsule_replay": 1}
    assert one["top_notebooks"][0] == {"notebook_id": "nb1", "name": "Load", "count": 4}
    p = one["passed_along"]
    assert p["pipeline_runs"] == 1 and p["with_default_lakehouse"] == 1 and p["source_modes"] == {"inline": 4, "pull": 1}
    assert p["compatibility_findings_by_class"] == {"native-portable": 3, "warning": 1} and p["context_keys_seen_in_handshakes"]["currentNotebookName"] == 1
    assert one["last_handshake"]["sdk_version"] == "0.1.0" and one["totals"]["submissions"] >= 7
    assert compute_metrics(stack.ctx, "bogus", now=NOW)["window"] == "24h"
    assert compute_metrics(stack.ctx, "7d", now=NOW)["counters"]["cancelled"] == 1


def test_counters_summary(stack: FabricStack) -> None:
    s = counters_summary(stack.ctx)
    assert s["submissions_total"] == sum(s["submissions_by_state"].values()) and s["handshakes_total"] >= 0


def test_readiness_checklist_lists_remediation_for_every_failing_item(stack: FabricStack) -> None:
    items = readiness(stack.fabric, stack.fabric.config())
    ids = [i["id"] for i in items]
    assert ids[:2] == ["enabled", "tenants"] and {"workspace_map", "broker", "public_url", "signing", "runtime", "handshake", "first_run"} <= set(ids)
    for i in items:
        assert {"id", "title", "ok", "required", "detail", "remediation"} <= set(i)
        assert (i["remediation"] is None) == i["ok"]
    by = {i["id"]: i for i in items}
    assert by["first_run"]["ok"] is True and by["workspace_map"]["ok"] is False
