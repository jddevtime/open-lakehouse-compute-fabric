# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``dataset.schema_changed``: published by the catalog when a re-sync finds a table's columns different from what it knew before (first sight is not a change)."""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
import pytest

from olcf.catalog.service import SCHEMA_EVENT_MAX_COLUMNS, column_diff
from olcf.core import dataset_events as dsev


def col(name: str, type_: str = "long", nullable: bool = True) -> dict[str, Any]:
    return {"name": name, "type": type_, "nullable": nullable}


# ---------------------------------------------------------------------------------------------------------- pure diff
def test_column_diff_added_removed_changed_type_and_nullability():
    before = [col("id"), col("amount", "double"), col("old"), col("flag", "boolean", True)]
    after = [col("id"), col("amount", "decimal(10,2)"), col("new", "string"), col("flag", "boolean", False)]
    added, removed, changed = column_diff(before, after)
    assert added == [{"name": "new", "type": "string", "nullable": True}]
    assert removed == [{"name": "old", "type": "long", "nullable": True}]
    assert changed == [{"name": "amount", "from": "double", "to": "decimal(10,2)", "nullable_from": True, "nullable_to": True},
                       {"name": "flag", "from": "boolean", "to": "boolean", "nullable_from": True, "nullable_to": False}]


def test_column_diff_names_compare_case_insensitively_and_unchanged_or_unknown_before_is_empty():
    assert column_diff([col("ID")], [col("id")]) == ([], [], [])
    assert column_diff([], [col("id"), col("x")]) == ([], [], [])          # no known previous schema: first sight, not a change
    assert column_diff([col("a")], [col("a")]) == ([], [], [])


# ---------------------------------------------------------------------------------------------------------- through the catalog
@pytest.fixture()
def world(dc):
    dc.workspace("ws1")
    lh = dc.lakehouse("ws1", "lh1")
    events: list[dict[str, Any]] = []
    unsub = dc.ctx.events.subscribe(lambda e: events.append(e) if e["type"] == "dataset.schema_changed" else None)
    yield lh, events
    unsub()


def refresh(dc, lh) -> dict[str, Any]:
    r = dc.post(f"/v1/lakehouses/{lh['id']}/refresh", headers=dc.admin)
    assert r.status_code == 200, r.text
    return r.json()


def test_first_sight_and_unchanged_resync_publish_nothing(dc, world):
    lh, events = world
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int64())}))
    refresh(dc, lh)
    refresh(dc, lh)
    assert events == []


def test_added_column_is_published_with_the_full_new_schema(dc, world):
    lh, events = world
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int64())}))
    refresh(dc, lh)
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([2], pa.int64()), "b": ["x"]}), mode="append", schema_mode="merge")
    refresh(dc, lh)
    (e,) = events
    p = e["payload"]
    assert e["workspace_id"] == lh["workspace_id"] and e["subject"] == lh["id"]
    at = p.pop("at")
    assert at[:4].isdigit()
    version = p.pop("version")
    assert version is not None
    assert p == {"workspace_id": lh["workspace_id"], "lakehouse_id": lh["id"], "table": "t",
                 "columns_added": [{"name": "b", "type": "string", "nullable": True}], "columns_removed": [], "columns_changed": [],
                 "columns": [{"name": "a", "type": "long", "nullable": True}, {"name": "b", "type": "string", "nullable": True}]}
    refresh(dc, lh)
    assert len(events) == 1, "a further sync without a change must not publish again"


def test_removed_and_retyped_columns(dc, world):
    lh, events = world
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int64()), "b": ["x"], "c": [1.5]}))
    refresh(dc, lh)
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int32()), "c": [1.5]}), mode="overwrite", schema_mode="overwrite")
    refresh(dc, lh)
    (e,) = events
    p = e["payload"]
    assert p["columns_removed"] == [{"name": "b", "type": "string", "nullable": True}]
    assert p["columns_changed"] == [{"name": "a", "from": "long", "to": "integer", "nullable_from": True, "nullable_to": True}]
    assert p["columns_added"] == []
    assert [c["name"] for c in p["columns"]] == ["a", "c"]


def test_a_failing_publisher_does_not_break_the_sync(dc, world, monkeypatch, caplog):
    lh, events = world
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int64())}))
    refresh(dc, lh)
    dc.write_table(lh["id"], "t", pa.table({"a": pa.array([1], pa.int64()), "b": ["x"]}), mode="overwrite", schema_mode="overwrite")
    bus = dc.ctx.events
    real = bus.publish
    dsev.reset_emission_state()

    def publish(type_, payload=None, **kw):
        if type_ == "dataset.schema_changed":
            raise RuntimeError("store down")
        return real(type_, payload, **kw)

    monkeypatch.setattr(bus, "publish", publish)
    with caplog.at_level(logging.WARNING, logger="olcf.catalog"):
        res = refresh(dc, lh)
    assert res["table_count"] == 1 and events == []
    assert dsev.emission_failures() == {"dataset.schema_changed": 1}
    entry = dc.get("/v1/catalog/tables", params={"include": "columns"}, headers=dc.admin).json()["items"][0]
    assert [c["name"] for c in entry["columns"]] == ["a", "b"], "the catalog itself still recorded the new schema"


def test_the_full_column_list_is_capped(dc, world, monkeypatch):
    lh, events = world
    n = SCHEMA_EVENT_MAX_COLUMNS + 20
    dc.write_table(lh["id"], "wide", pa.table({"c0": pa.array([1], pa.int64())}))
    refresh(dc, lh)
    dc.write_table(lh["id"], "wide", pa.table({f"c{i}": pa.array([1], pa.int64()) for i in range(n)}), mode="overwrite", schema_mode="overwrite")
    refresh(dc, lh)
    (e,) = events
    assert len(e["payload"]["columns"]) == SCHEMA_EVENT_MAX_COLUMNS
    assert len(e["payload"]["columns_added"]) == n - 1, "the diff itself is never truncated"
