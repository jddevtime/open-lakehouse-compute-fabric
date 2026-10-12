# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Schema change, end to end on real Delta tables: a column is added / dropped behind the platform's back, the catalog is synchronised, the lineage is scanned again.

Real here: the Delta tables (``deltalake`` rewrites them), the catalog sync that notices the difference and publishes ``dataset.schema_changed``, the durable inbox, the inbox
processor, the local scan, the snapshot diff, the governance background pass that invalidates certifications. Each test uses its own certified gold table of the sample.

Ordering matters for *who* records a change, and the tests pin both orders down instead of pretending they are the same:

* the scan runs before the inbox processes the event (a busy inbox): the local scan sees the new column, so the **local** snapshot diff lists it;
* the inbox processes the event first (the normal case, seconds later): the column is recorded by the run-time observation, the graph is right immediately, and the later local
  scan has nothing left to add (its diff shows only the changed counters). Snapshot diffs are per source; the column then appears in the version history of the table.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import select

from olcf.db import schema as S
from olcf.intelligence.schema import asset_versions, inbox
from tests.intelligence.system.conftest import Platform, build_platform

NEW_COLUMN = "promo_flag"


@pytest.fixture(scope="module")
def p(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Platform]:
    with build_platform(tmp_path_factory.mktemp("schema"), None, install_sample=True) as platform:
        yield platform


def _table_path(p: Platform, table: str) -> Path:
    return next(Path(str(p.ctx.settings.path("storage"))).rglob(f"{table}/_delta_log")).parent


def _rewrite(p: Platform, table: str, *, add: str | None = None, drop: str | None = None) -> None:
    """Change the schema of a Delta table the way a job would: rewrite it with another column set."""
    path = _table_path(p, table)
    data = DeltaTable(str(path)).to_pyarrow_table()
    if add:
        data = data.append_column(add, pa.array([False] * data.num_rows, pa.bool_()))
    if drop:
        data = data.drop_columns([drop])
    write_deltalake(str(path), data, mode="overwrite", schema_mode="overwrite")


def _sync(p: Platform) -> dict[str, Any]:
    return p.post("/v1/catalog/sync", None, expect=200, workspace_id=p.sample["workspace_id"])  # type: ignore[attr-defined]


def _local_snapshots(p: Platform) -> list[dict[str, Any]]:
    src = next(s for s in p.get("/v1/intelligence/sources")["items"] if s["kind"] == "local")
    return list(p.get("/v1/intelligence/snapshots", source_id=src["id"])["items"])


def _cert(p: Platform, asset_id: str) -> dict[str, Any]:
    return p.get(f"/v1/governance/assets/{asset_id}/certification")


def _events(p: Platform, type_: str, **where: Any) -> list[dict[str, Any]]:
    with p.ctx.db.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(select(S.events).where(S.events.c.type == type_).order_by(S.events.c.seq))]
    return [r for r in rows if all(r["payload"].get(k) == v for k, v in where.items())]


def _pending(p: Platform, type_: str) -> int:
    with p.ctx.db.connect() as c:
        return len(c.execute(select(inbox.c.seq).where(inbox.c.type == type_, inbox.c.status == "pending")).all())


def test_an_unchanged_rescan_changes_nothing_and_keeps_every_certification(p: Platform) -> None:
    gold = {n: p.asset("table", n) for n in ("gold_daily_sales", "gold_category_summary", "gold_customer_ltv")}
    assert all(_cert(p, a)["state"] == "certified" for a in gold.values())
    before = _local_snapshots(p)[0]["id"]
    scan = p.scan()
    p.intel.governance.tick()
    diff = p.get("/v1/intelligence/snapshots/diff", **{"from": before, "to": scan["id"]})
    assert diff["summary"] == {"assets_added": 0, "assets_removed": 0, "assets_changed": 0, "edges_added": 0, "edges_removed": 0, "edges_changed": 0}
    assert all(_cert(p, a)["state"] == "certified" for a in gold.values())
    assert _events(p, "dataset.schema_changed") == []


def test_scan_before_the_event_is_processed_the_local_diff_lists_the_new_column(p: Platform) -> None:
    table = "gold_customer_ltv"
    asset = p.asset("table", table)
    before = _local_snapshots(p)[0]["id"]
    _rewrite(p, table, add=NEW_COLUMN)
    synced = _sync(p)
    assert table in next(lh for lh in synced["lakehouses"])["updated"]
    # the catalog noticed and told everybody: the platform event is persisted and waits in the Intelligence inbox
    ev = _events(p, "dataset.schema_changed", table=table)
    assert len(ev) == 1 and [c["name"] for c in ev[0]["payload"]["columns_added"]] == [NEW_COLUMN] and ev[0]["payload"]["columns_removed"] == []
    assert _pending(p, "dataset.schema_changed") == 1
    with pytest.raises(AssertionError):                                            # the graph does not know the column yet
        p.asset("column", NEW_COLUMN, parent=table, status=None)
    assert _cert(p, asset)["state"] == "certified"
    scan = p.scan()
    assert scan["status"] == "succeeded"
    diff = p.get("/v1/intelligence/snapshots/diff", **{"from": before, "to": scan["id"]})
    assert [a["name"] for a in diff["assets"]["added"]] == [NEW_COLUMN] and diff["assets"]["added"][0]["type"] == "column"
    changed = next(a for a in diff["assets"]["changed"] if a["id"] == asset)
    assert changed["changes"]["columns_added"] == [NEW_COLUMN]
    # the governance pass reverts the certification, with a reason, and announces it
    result = p.intel.governance.tick()
    assert result["invalidated"] >= 1
    cert = _cert(p, asset)
    assert cert["state"] == "draft" and "schema changed" in cert["invalidated_reason"] and cert["invalidated_at"] and cert["certified_at"]
    changes = _events(p, "governance.certification.changed", subject_id=asset, state="draft")
    assert len(changes) == 1 and changes[0]["payload"]["previous"] == "certified" and "schema" in changes[0]["payload"]["reason"]
    # the late event changes nothing and nothing is announced twice
    p.drain()
    assert _pending(p, "dataset.schema_changed") == 0
    p.intel.governance.tick()
    assert len(_events(p, "governance.certification.changed", subject_id=asset, state="draft")) == 1
    assert p.asset("column", NEW_COLUMN, parent=table)                              # exactly one such column
    # the other certified tables are untouched
    assert _cert(p, p.asset("table", "gold_category_summary"))["state"] == "certified"


def test_event_processed_first_the_graph_is_right_before_any_scan(p: Platform) -> None:
    table = "gold_daily_sales"
    asset = p.asset("table", table)
    _rewrite(p, table, add=NEW_COLUMN)
    _sync(p)
    p.drain()                                                                       # seconds later in a real deployment
    column = p.asset("column", NEW_COLUMN, parent=table, status=None)               # known without any scan
    assert p.get(f"/v1/intelligence/assets/{column}")["name"] == NEW_COLUMN
    p.intel.governance.tick()
    assert _cert(p, asset)["state"] == "draft"
    assert len(_events(p, "governance.certification.changed", subject_id=asset, state="draft")) == 1
    before = _local_snapshots(p)[0]["id"]
    scan = p.scan()
    diff = p.get("/v1/intelligence/snapshots/diff", **{"from": before, "to": scan["id"]})
    mine = [a for a in diff["assets"]["changed"] if a["id"] == asset]
    assert not [a for a in diff["assets"]["added"] if a["name"] == NEW_COLUMN]      # attributed to the run-time observation, not to this local scan
    assert not mine or "columns_added" not in mine[0]["changes"]
    with p.ctx.db.connect() as c:                                                   # ... which is in the version history of the table
        rows = [dict(r._mapping) for r in c.execute(select(asset_versions).where(asset_versions.c.asset_id == asset).order_by(asset_versions.c.id))]
    assert any(NEW_COLUMN in (r["changes"] or {}).get("columns_added", []) for r in rows)
    p.intel.governance.tick()
    assert len(_events(p, "governance.certification.changed", subject_id=asset, state="draft")) == 1     # still announced once


def test_a_dropped_column_is_a_removal_in_the_diff_and_reverts_the_certification(p: Platform) -> None:
    table = "gold_category_summary"
    asset = p.asset("table", table)
    assert _cert(p, asset)["state"] == "certified"
    before = _local_snapshots(p)[0]["id"]
    _rewrite(p, table, drop="margin_pct")
    scan = p.scan()                                                                 # no sync, no event: the scan alone must notice
    diff = p.get("/v1/intelligence/snapshots/diff", **{"from": before, "to": scan["id"]})
    changed = next(a for a in diff["assets"]["changed"] if a["id"] == asset)
    assert changed["changes"]["columns_removed"] == ["margin_pct"]
    p.intel.governance.tick()
    cert = _cert(p, asset)
    assert cert["state"] == "draft" and "schema changed" in cert["invalidated_reason"]
    # a re-certification records the new schema and survives the next scan
    p.http.put(f"/v1/governance/assets/{asset}/certification", json={"state": "certified", "comment": "reviewed after the change"}, headers=p.admin)
    p.scan()
    p.intel.governance.tick()
    assert _cert(p, asset)["state"] == "certified"
