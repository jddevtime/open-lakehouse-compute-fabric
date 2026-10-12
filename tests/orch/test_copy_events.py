# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The copy activity publishes ``dataset.read`` (source) and ``dataset.written`` (sink) after a successful copy; a failing publisher never fails the copy."""

from __future__ import annotations

from typing import Any

import pytest

from olcf.core import dataset_events as dsev
from tests.orch.orch_support import Inproc

CSV = "id,name,score\n1,ann,9.5\n2,bob,7.25\n3,cy,8\n"


def A(name: str, **props: Any) -> dict[str, Any]:
    return {"name": name, "type": "copy", "typeProperties": props}


def put(ip: Inproc, lh: Any, path: str, data: str) -> None:
    ip.storage.write_bytes(lh, path, data.encode())


@pytest.fixture()
def seen(ip: Inproc):
    events: list[dict[str, Any]] = []
    unsub = ip.ctx.events.subscribe(lambda e: events.append(e) if e["type"].startswith("dataset.") else None)
    yield events
    unsub()


def run_copy(ip: Inproc, act: dict[str, Any], name: str = "copy-events") -> tuple[dict[str, Any], dict[str, Any]]:
    p = ip.pipeline(name + "-" + act["name"].lower(), [act])
    run = ip.wait(ip.run(p)["id"], timeout=30)
    return run, p


def test_file_copy_publishes_read_and_written_with_the_pipeline_as_process(ip: Inproc, seen) -> None:
    src, dst = ip.lakehouse("cesrc"), ip.lakehouse("cedst")
    put(ip, src, "Files/in/a.csv", CSV)
    put(ip, src, "Files/in/sub/b.csv", CSV)
    run, p = run_copy(ip, A("C", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out", "mode": "overwrite"}))
    assert run["state"] == "succeeded", run
    assert [e["type"] for e in seen] == ["dataset.read", "dataset.written"]
    read, written = seen
    process = {"kind": "pipeline", "item_id": p["id"], "job_id": None, "query_id": None, "name": f"{p['name']}/C", "run_id": run["id"]}
    for e in seen:
        assert e["workspace_id"] == run["workspace_id"] and e["subject"] == run["id"]
        assert e["payload"]["process"] == process and e["payload"]["workspace_id"] == run["workspace_id"] and e["payload"]["engine"] == "python"
        assert e["payload"]["statement_hash"] is None and e["payload"]["at"][:2] == "20"
    assert read["payload"]["mode"] is None
    assert read["payload"]["datasets"] == [{"kind": "file", "lakehouse_id": src.id, "name": None, "path": "Files/in", "uri": None, "format": "binary"},
                                           {"kind": "file", "lakehouse_id": src.id, "name": None, "path": "Files/in/sub", "uri": None, "format": "binary"}]
    assert written["payload"]["mode"] == "overwrite"
    assert [(d["lakehouse_id"], d["path"]) for d in written["payload"]["datasets"]] == [(dst.id, "Files/out"), (dst.id, "Files/out/sub")]


def test_table_sink_uses_delta_and_the_sink_mode(ip: Inproc, seen) -> None:
    src, dst = ip.lakehouse("cesrc2"), ip.lakehouse("cedst2")
    put(ip, src, "Files/in/people.csv", CSV)
    for mode, want in (("overwrite", "overwrite"), ("append", "append")):
        seen.clear()
        run, _ = run_copy(ip, A(f"T{mode}", source={"lakehouse": src.name, "path": "Files/in/people.csv", "format": "csv"}, sink={"lakehouse": dst.name, "table": "people", "mode": mode}))
        assert run["state"] == "succeeded", run
        read, written = seen
        assert read["payload"]["datasets"] == [{"kind": "file", "lakehouse_id": src.id, "name": None, "path": "Files/in", "uri": None, "format": "csv"}]
        assert written["payload"]["datasets"] == [{"kind": "table", "lakehouse_id": dst.id, "name": "people", "path": None, "uri": None, "format": "delta"}]
        assert written["payload"]["mode"] == want
    seen.clear()
    run, _ = run_copy(ip, A("TT", source={"lakehouse": dst.name, "table": "people"}, sink={"lakehouse": dst.name, "table": "people_copy", "mode": "overwrite"}))
    assert run["state"] == "succeeded"
    assert seen[0]["payload"]["datasets"] == [{"kind": "table", "lakehouse_id": dst.id, "name": "people", "path": None, "uri": None, "format": "delta"}]
    assert seen[1]["payload"]["datasets"][0]["name"] == "people_copy"


def test_failed_copy_publishes_nothing(ip: Inproc, seen) -> None:
    src, dst = ip.lakehouse("cesrc3"), ip.lakehouse("cedst3")
    run, _ = run_copy(ip, A("Missing", source={"lakehouse": src.name, "path": "Files/nope"}, sink={"lakehouse": dst.name, "path": "Files/x"}))
    assert run["state"] == "failed"
    assert seen == []


def test_publisher_failure_does_not_fail_the_copy_or_its_lineage(ip: Inproc, monkeypatch) -> None:
    src, dst = ip.lakehouse("cesrc4"), ip.lakehouse("cedst4")
    put(ip, src, "Files/in/a.csv", CSV)
    real = ip.ctx.events.publish
    dsev.reset_emission_state()

    def publish(type_, payload=None, **kw):
        if type_.startswith("dataset."):
            raise RuntimeError("event store down")
        return real(type_, payload, **kw)

    monkeypatch.setattr(ip.ctx.events, "publish", publish)
    run, _ = run_copy(ip, A("C", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out"}))
    assert run["state"] == "succeeded", run
    assert ip.storage.stat(dst, "Files/out/a.csv").size == len(CSV)
    assert dsev.emission_failures() == {"dataset.read": 1, "dataset.written": 1}
    graph = ip.ctx.service("catalog").lineage(ip.admin, {"kind": "file", "lakehouse_id": dst.id, "path": "Files/out"}, direction="up")
    assert graph["edges"], "the catalog lineage edge is still recorded"
