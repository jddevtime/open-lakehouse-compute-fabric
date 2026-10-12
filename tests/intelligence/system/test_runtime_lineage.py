# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Live runtime lineage, end to end: a REAL notebook / script run reads table A and writes table B, and the graph learns it from the run.

Nothing is stubbed on the way: the application is served by a real uvicorn with every module, the job is placed on a real worker agent subprocess, the runner executes the
code in a real ``ipykernel`` (notebook) / a real child process (script), the in-job lineage probe reports what the code really touched, the control plane validates and
republishes the lines as ``dataset.*`` events, the Intelligence inbox stores them and the inbox processor turns them into verified edges.

What is *not* real: the runtime directory is the NODE package's own Spark-less look-alike (``tests.node.fakes.make_fake_runtime``; the interpreter is this one), so the engine
is ``python`` (deltalake + DuckDB). Spark itself (``spark.table`` / ``spark.sql`` hooks) is covered by ``tests/node/test_lineage_probe.py`` and the ``spark`` shard, not here.

The notebook builds its table names at run time on purpose: static analysis has to find nothing (and say why), so every edge below can only come from the run.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import select

from olcf.intelligence.schema import edges as edges_t
from olcf.intelligence.schema import gaps as gaps_t
from olcf.intelligence.schema import inbox
from tests.intelligence.system.conftest import RUNTIME_ID, LivePlatform, build_live_platform

NOTEBOOK = "Dynamic_Rollup"
ROLLUP = "rollup_8cd892"           # "rollup_" + md5(b"store")[:6], computed by the notebook at run time

NB_SOURCE = '''import hashlib
import os
from pathlib import Path

import duckdb
from deltalake import DeltaTable, write_deltalake
from olcf.runtime_lib import paths

tables = Path(paths.resolve("Tables/fact_sales", for_="local")).parent
src_name = next(n for n in sorted(os.listdir(tables)) if n.startswith("fact_s"))
dst_name = "rollup_" + hashlib.md5(b"store").hexdigest()[:6]
con = duckdb.connect()
con.register(src_name, DeltaTable(str(tables / src_name)).to_pyarrow_dataset())
con.execute(f"CREATE TABLE {dst_name} AS SELECT store_id, sum(net_amount) AS revenue, count(*) AS lines FROM {src_name} GROUP BY store_id")
write_deltalake(str(tables / dst_name), con.table(dst_name).fetch_arrow_table(), mode="overwrite")
print("written", dst_name)
'''

JOB_SOURCE = '''import os
from pathlib import Path

from deltalake import DeltaTable, write_deltalake
from olcf.runtime_lib import paths

tables = Path(paths.resolve("Tables/dim_store", for_="local")).parent
name = os.environ.get("OLCF_TARGET") or "stores_" + "copy"
data = DeltaTable(str(tables / "dim_store")).to_pyarrow_table()
write_deltalake(str(tables / name), data, mode="overwrite")
print("copied", data.num_rows, "rows to", name)
'''

CRASHING_JOB = '''from pathlib import Path

from deltalake import DeltaTable, write_deltalake
from olcf.runtime_lib import paths

tables = Path(paths.resolve("Tables/dim_product", for_="local")).parent
data = DeltaTable(str(tables / "dim_product")).to_pyarrow_table()
raise RuntimeError("the job fails after reading and before writing")
write_deltalake(str(tables / "never_written"), data)
'''


class Run:
    """What the module-scoped scenario observed (before / after the run), kept for the individual tests."""

    platform: LivePlatform
    workspace_id: str
    item_id: str
    notebook: str
    job: dict[str, Any]
    logs: str
    static_edges: list[tuple[str, str]]
    static_gaps: list[tuple[str, str]]
    inbox_before: int


def _edges_of(p: LivePlatform, asset_id: str, direction: str, **params: Any) -> list[dict[str, Any]]:
    return list(p.lineage(asset_id, direction=direction, **params)["edges"])


def _inbox_rows(p: LivePlatform, job_id: str) -> list[dict[str, Any]]:
    with p.ctx.db.connect() as c:
        return [dict(r._mapping) for r in c.execute(select(inbox).order_by(inbox.c.seq)) if (r.payload or {}).get("process", {}).get("job_id") == job_id
                or (r.payload or {}).get("job_id") == job_id]


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Run]:
    with build_live_platform(tmp_path_factory.mktemp("live"), install_sample=True, agent=True) as p:
        r = Run()
        r.platform = p
        r.workspace_id = p.sample["workspace_id"]
        nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}},
              "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": NB_SOURCE}]}
        up = p.http.post(f"/v1/workspaces/{r.workspace_id}/notebooks/import", headers=p.admin, files={"file": (f"{NOTEBOOK}.ipynb", json.dumps(nb).encode(), "application/json")})
        assert up.status_code == 201, up.text
        r.item_id = up.json()["items"][0]["id"]
        p.scan()                                                   # discovery sees the notebook and analyses it statically
        r.notebook = p.asset("notebook", NOTEBOOK)
        with p.ctx.db.connect() as c:
            r.static_edges = [(x.type, x.confidence) for x in c.execute(select(edges_t.c.type, edges_t.c.confidence).where(edges_t.c.src_id == r.notebook))]
            r.static_gaps = [(x.kind, x.detail) for x in c.execute(select(gaps_t.c.kind, gaps_t.c.detail).where(gaps_t.c.asset_id == r.notebook))]
        p.drain()
        with p.ctx.db.connect() as c:
            r.inbox_before = len(c.execute(select(inbox.c.seq)).all())
        started = p.post(f"/v1/notebooks/{r.item_id}/run", {"engine": "python", "runtime": {"runtime_id": RUNTIME_ID}, "default_lakehouse": {"name": "retail_lakehouse"}}, expect=202)
        r.job = p.wait_job(started["id"], 180)
        out = p.get(f"/v1/jobs/{started['id']}/output")                  # a notebook's prints are in its output notebook
        r.logs = "\n".join("".join(o.get("text", "")) for c in out["cells"] for o in c.get("outputs", []))
        p.drain()
        yield r


def test_static_analysis_found_nothing_and_said_why(run: Run) -> None:
    """The precondition of every test below: before the run the graph knows no table this notebook touches."""
    assert run.static_edges == []
    kinds = {k for k, _ in run.static_gaps}
    assert kinds == {"dynamic_reference"}, run.static_gaps
    assert any("built at run time" in d or "not a compile-time constant" in d for _, d in run.static_gaps)


def test_the_notebook_really_ran_on_the_worker(run: Run) -> None:
    assert run.job["state"] == "succeeded", run.job.get("failure")
    assert run.job["kind"] == "notebook" and run.job["item_id"] == run.item_id and run.job["driver_worker_id"]
    assert f"written {ROLLUP}" in run.logs


def test_runtime_events_reached_the_inbox_and_were_processed(run: Run) -> None:
    rows = _inbox_rows(run.platform, run.job["id"])
    by_type: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_type.setdefault(r["type"], []).append(r)
    assert {"dataset.read", "dataset.written", "dataset.statement"} <= set(by_type), sorted(by_type)
    for t in ("dataset.read", "dataset.written", "dataset.statement"):
        assert all(r["status"] == "done" and r["error"] is None for r in by_type[t]), (t, by_type[t])
    # job.state_changed rows are stored too (there is nothing in them for the graph; they are 'skipped', not failed)
    assert {r["status"] for r in by_type.get("job.state_changed", [])} <= {"skipped", "done"}
    written = by_type["dataset.written"][0]["payload"]
    assert written["process"]["kind"] == "notebook" and written["process"]["item_id"] == run.item_id and written["datasets"][0]["name"] == ROLLUP
    # the SQL text is only carried in-process: the statement row has it, the platform event table does not
    assert "CREATE TABLE" in by_type["dataset.statement"][0]["payload"]["sql"]
    assert run.platform.intel.inbox_stats()["failed"] == 0


def test_verified_reads_from_and_writes_to_edges_with_runtime_evidence(run: Run) -> None:
    p = run.platform
    down = _edges_of(p, run.notebook, "down")
    writes = [e for e in down if e["type"] == "WRITES_TO"]
    assert len(writes) == 1 and writes[0]["confidence"] == "verified" and writes[0]["src"] == run.notebook
    table = writes[0]["dst"]
    up = _edges_of(p, run.notebook, "up")
    reads = [e for e in up if e["type"] == "READS_FROM" and e["src"] == run.notebook]
    assert len(reads) == 1 and reads[0]["confidence"] == "verified"
    assert reads[0]["dst"] == p.asset("table", "fact_sales")                    # A: the table the code really read
    assert p.get(f"/v1/intelligence/assets/{table}")["name"] == ROLLUP           # B: the table the code really wrote
    for e in (writes[0], reads[0]):
        detail = p.get(f"/v1/intelligence/edges/{e['id']}")
        kinds = {ev["kind"] for ev in detail["evidence"]}
        assert "runtime" in kinds, detail["evidence"]
        rt = [x for x in detail["evidence"] if x["kind"] == "runtime"]
        assert all(x["confidence"] == "verified" and x["ref"] == run.job["id"] for x in rt), rt
        assert any(x["location"].get("item_id") == run.item_id and x["location"].get("job_id") == run.job["id"] for x in rt), rt
    # the lineage question a person asks: where does rollup come from, through the notebook, back to the sample's source
    origin = p.origin(table)
    names = {n["name"] for n in origin["nodes"]}
    assert {NOTEBOOK, "fact_sales", "sample generator"} <= names
    assert origin["has_upstream"] and origin["paths"]
    through = [x for x in origin["paths"] if run.notebook in x["nodes"]]
    assert through and {x["confidence"] for x in through} <= {"verified", "derived"}
    # impact of fact_sales now includes the new table, found by running code and not by any scan
    impact = p.impact(p.asset("table", "fact_sales"))
    assert ROLLUP in json.dumps(impact)


def test_column_lineage_from_the_statement_is_verified(run: Run) -> None:
    p = run.platform
    revenue = p.asset("column", "revenue", parent=ROLLUP, status=None)
    store = p.asset("column", "store_id", parent=ROLLUP, status=None)
    net = p.asset("column", "net_amount", parent="fact_sales")
    up = p.lineage(revenue, direction="up", level="column")
    col_edges = [e for e in up["edges"] if e["type"] == "TRANSFORMS"]
    assert any(e["src"] == net and e["dst"] == revenue and e["confidence"] == "verified" for e in col_edges), col_edges
    store_up = p.lineage(store, direction="up", level="column")
    assert any(e["src"] == p.asset("column", "store_id", parent="fact_sales") and e["confidence"] == "verified" for e in store_up["edges"] if e["type"] == "TRANSFORMS")
    # count(*) reads no particular column: the statement gives it no column-level source (the honest answer, not an invented one) and no column asset until a scan sees the schema
    assert not [n for n in up["nodes"] if n["name"] == "lines"]
    origin = p.origin(revenue, level="column")
    assert origin["paths"] and any(net in x["nodes"] for x in origin["paths"])
    evidence = p.get(f"/v1/intelligence/edges/{next(e for e in col_edges if e['dst'] == revenue)['id']}")["evidence"]
    assert {ev["kind"] for ev in evidence} == {"runtime"} and evidence[0]["ref"] == run.job["id"]


def test_the_next_scan_discovers_the_table_and_keeps_the_runtime_edges(run: Run) -> None:
    """The table was only a placeholder (code wrote it, no scan had seen it); the next scan gives it a real identity, schema and path - the same asset, same edges."""
    p = run.platform
    placeholder = p.asset("table", ROLLUP, status="placeholder")
    scan = p.scan(mode="full")
    assert scan["status"] == "succeeded", scan
    assert p.asset("table", ROLLUP) == placeholder                                     # promoted in place, not duplicated
    assert p.get(f"/v1/intelligence/assets/{placeholder}")["path"].endswith(f"retail_lakehouse / {ROLLUP}")
    cols = {n: p.asset("column", n, parent=ROLLUP) for n in ("store_id", "revenue", "lines")}               # 'lines' only exists now: the scan read the Delta schema
    assert not [e for e in p.lineage(cols["lines"], direction="up", level="column")["edges"] if e["type"] == "TRANSFORMS" and e["dst"] == cols["lines"]]
    # a FULL scan of the local source swept nothing that a run had reported
    after = {(e["type"], e["confidence"]) for e in _edges_of(p, run.notebook, "down") + _edges_of(p, run.notebook, "up")}
    assert {("WRITES_TO", "verified"), ("READS_FROM", "verified")} <= after
    assert p.origin(placeholder)["has_upstream"]


def test_a_plain_job_without_a_notebook_item_is_a_process_too(run: Run) -> None:
    p = run.platform
    j = p.submit(JOB_SOURCE, workspace_id=run.workspace_id, lakehouse="retail_lakehouse", name="copy-stores")
    done = p.wait_job(j["id"])
    assert done["state"] == "succeeded", done.get("failure")
    p.drain()
    job_asset = p.asset("script", "copy-stores", status=None)
    writes = [e for e in _edges_of(p, job_asset, "down") if e["type"] == "WRITES_TO"]
    reads = [e for e in _edges_of(p, job_asset, "up") if e["type"] == "READS_FROM" and e["src"] == job_asset]
    assert len(writes) == 1 and writes[0]["confidence"] == "verified"
    assert [e["dst"] for e in reads] == [p.asset("table", "dim_store")] and reads[0]["confidence"] == "verified"
    assert p.get(f"/v1/intelligence/assets/{writes[0]['dst']}")["name"] == "stores_copy"


def test_a_job_that_fails_after_reading_records_the_read_and_no_write(run: Run) -> None:
    """Writes are reported after they succeeded; a crash before the write must not leave a WRITES_TO edge."""
    p = run.platform
    j = p.submit(CRASHING_JOB, workspace_id=run.workspace_id, lakehouse="retail_lakehouse", name="crashing-reader")
    done = p.wait_job(j["id"])
    assert done["state"] == "failed" and done["failure"]["category"] == "user-code"
    p.drain()
    job_asset = p.asset("script", "crashing-reader", status=None)
    assert [e for e in _edges_of(p, job_asset, "down") if e["type"] == "WRITES_TO"] == []
    assert [e["confidence"] for e in _edges_of(p, job_asset, "up") if e["type"] == "READS_FROM" and e["src"] == job_asset] == ["verified"]
    assert not [r for r in _inbox_rows(p, done["id"]) if r["type"] == "dataset.written"]


def test_replaying_the_same_events_changes_nothing(run: Run) -> None:
    """Idempotency of the whole path: the same run observed again (duplicated delivery) neither duplicates edges nor assets."""
    p = run.platform
    before = {(e["id"], e["observation_count"] if "observation_count" in e else 0) for e in _edges_of(p, run.notebook, "down")}
    rows = [r for r in _inbox_rows(p, run.job["id"]) if r["type"] in ("dataset.read", "dataset.written", "dataset.statement")]
    for r in rows:
        payload = {k: v for k, v in r["payload"].items() if k != "_meta"}
        p.ctx.events.publish(r["type"], payload, workspace_id=r["workspace_id"], subject=run.job["id"], persist=r["type"] != "dataset.statement")
    p.drain()
    after = {(e["id"], e["observation_count"] if "observation_count" in e else 0) for e in _edges_of(p, run.notebook, "down")}
    assert {i for i, _ in after} == {i for i, _ in before}
    assert len([e for e in _edges_of(p, run.notebook, "down") if e["type"] == "WRITES_TO"]) == 1
