# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``local`` connector against a real platform: workspaces, lakehouses, Delta tables (read with delta-rs, never Spark), items, definitions handed to the analyzers, sweeps."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pytest

from olcf.intelligence import model as M
from olcf.intelligence.model import AssetRef, EdgeFact, Evidence, Facts
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table

VALUES = ["user0@example.com", "user3@example.com", "100-45-6789", "Oslo", "Kyiv"]


def dump_graph(disc: Any) -> str:
    """Everything the graph stores about assets, as one string (to prove that no data value is in it)."""
    from sqlalchemy import select

    from olcf.intelligence.schema import asset_versions, edges, gaps

    with disc.ctx.db.connect() as c:
        parts = [json.dumps([dict(r._mapping) for r in c.execute(select(t))], default=str) for t in (asset_versions, edges, gaps)]
    return json.dumps(disc.assets(), default=str) + "".join(parts)


def test_full_scan_reports_the_platform(disc):
    disc.lakehouse("alpha", "lh1")
    disc.write("people", people_table())
    row = disc.scan()
    assert row["status"] == "succeeded" and row["mode"] == "full"
    assert row["stats"]["facts"]["assets"] >= 8 and row["stats"]["sweep"]["assets_removed"] == 0
    types = {(a["type"], a["name"]) for a in disc.assets()}
    assert {("workspace", "alpha"), ("lakehouse", "lh1"), ("table", "people"), ("column", "email")} <= types
    table = disc.asset("people", "table")
    attrs = table["attrs"]
    assert attrs["format"] == "delta" and attrs["column_count"] == 5 and attrs["row_count"] == 6 and attrs["source_kind"] == "local"
    assert attrs["profile"]["columns"]["city"]["null_count"] == 1                      # statistics come from the Delta log, not from reading data
    assert attrs["profile"]["columns"]["email"].get("min") is None                    # no min / max of text or sensitive columns
    assert table["workspace_id"] == disc.ws["id"] and table["source_id"] == disc.source()["id"]
    lh = disc.asset("lh1", "lakehouse")
    assert table["parent_id"] == lh["id"]
    assert disc.asset("email", "column")["parent_id"] == table["id"]


def test_sensitive_columns_get_suggestions_and_no_value_is_stored(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.scan()
    email = disc.asset("email", "column")["attrs"]["suggested_classifications"]
    assert any(s["classification"] == "email" for s in email) and all("confidence" in s and "evidence" in s for s in email)
    assert any(s["classification"] == "national_id" for s in disc.asset("ssn", "column")["attrs"]["suggested_classifications"])
    assert "suggested_classifications" not in disc.asset("amount", "column")["attrs"]
    assert disc.asset("people", "table")["attrs"]["sensitive_columns"]
    blob = dump_graph(disc)
    for v in VALUES:
        assert v not in blob, f"a data value ({v}) reached the graph"


def test_second_scan_is_idempotent_and_schema_changes_are_versioned(disc):
    from sqlalchemy import func, select

    from olcf.intelligence.schema import asset_versions

    disc.lakehouse()
    disc.write("people", people_table())
    disc.scan()
    tid = disc.asset("people", "table")["id"]

    def versions() -> int:
        with disc.ctx.db.connect() as c:
            return int(c.execute(select(func.count()).select_from(asset_versions).where(asset_versions.c.asset_id == tid)).scalar() or 0)

    before = versions()
    again = disc.scan()
    assert again["status"] == "succeeded" and again["stats"]["apply"]["assets_created"] == 0 and again["stats"]["apply"]["assets_updated"] == 0
    assert versions() == before                                                        # volatile statistics do not create versions
    disc.write("people", pa.table({"id": pa.array([99], pa.int64()), "email": ["x@example.org"], "ssn": ["000-00-0000"], "amount": [1.0], "city": ["Bergen"], "country": ["NO"]}),
               mode="append", schema_mode="merge")                                       # a new column through a schema-evolving append
    changed = disc.scan(mode="incremental")
    assert changed["stats"]["apply"]["assets_updated"] >= 1 and versions() == before + 1
    assert disc.asset("country", "column")["parent_id"] == tid


def test_full_scan_marks_dropped_tables_removed_and_incremental_does_not(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.write("orders", pa.table({"id": pa.array([1, 2], pa.int64())}))
    disc.scan()
    lh = disc.ctx.service("storage").get_lakehouse(disc.lh["id"])
    disc.ctx.service("storage").tables.drop(lh, "orders")
    inc = disc.scan(mode="incremental")
    assert inc["status"] == "succeeded" and "sweep" not in inc["stats"]
    assert disc.asset("orders", "table")["status"] == "active"                          # an incremental scan never removes anything
    full = disc.scan(mode="full")
    assert full["stats"]["sweep"]["assets_removed"] >= 2                                # the table and its column
    assert disc.asset("orders", "table", status="removed")["removed_scan"] == full["id"]
    assert disc.asset("people", "table")["status"] == "active"


def test_scan_with_a_blocking_gap_is_partial_and_never_sweeps(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.write("orders", pa.table({"id": pa.array([1], pa.int64())}))
    disc.scan()
    lh = disc.ctx.service("storage").get_lakehouse(disc.lh["id"])
    disc.ctx.service("storage").tables.drop(lh, "orders")
    # a table cap smaller than the number of tables is a "may have missed objects" gap
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"max_tables_per_lakehouse": 1}})
    disc.write("zeta", pa.table({"id": pa.array([1], pa.int64())}))
    part = disc.scan()
    assert part["status"] == "partial" and "sweep" not in part["stats"] and part["stats"]["blocking_gaps"] >= 1
    assert "removal sweep was skipped" in part["error"]
    assert disc.asset("orders", "table")["status"] == "active"                          # not forgotten merely because the scan could not see everything
    gap = next(g for g in disc.gaps() if g["kind"] == "not_inspected" and "only the first 1" in g["detail"])
    assert gap["remediation"] and gap["source_id"] == disc.source()["id"]
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"max_tables_per_lakehouse": None}})
    done = disc.scan()
    assert done["status"] == "succeeded" and done["stats"]["sweep"]["assets_removed"] >= 2
    assert not [g for g in disc.gaps() if "only the first 1" in g["detail"]]            # the next clean full scan resolves the gap


def test_informational_gaps_do_not_stop_the_sweep(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"workspace_ids": [disc.ws["id"], "ghost-workspace"]}})
    row = disc.scan()
    assert row["status"] == "succeeded" and row["stats"]["gaps_reported"] == 1 and row["stats"]["blocking_gaps"] == 0 and "sweep" in row["stats"]
    assert any("ghost-workspace" in g["detail"] for g in disc.gaps())


def test_request_level_workspace_filter_on_a_full_scan_is_partial(disc):
    disc.lakehouse("alpha", "lh1")
    disc.write("people", people_table())
    other = disc.intel.workspace("beta")
    row = disc.scan(workspace_filter=[disc.ws["id"]])
    assert row["status"] == "partial" and "limited to some workspaces" in row["error"] and "sweep" not in row["stats"]
    assert not [a for a in disc.assets() if a["type"] == "workspace" and a["name"] == "beta"]
    assert other["id"] != disc.ws["id"]
    with pytest.raises(Exception, match="workspace_ids"):
        disc.scan(workspace_filter=[])


def test_first_incremental_scan_is_promoted_to_full(disc):
    disc.lakehouse()
    row = disc.scan(mode="incremental")
    assert row["mode"] == "full" and row["stats"]["promoted_to_full"] is True
    assert disc.scan(mode="incremental")["mode"] == "incremental"


# ------------------------------------------------------------------------------------------------ definitions and the analyzers
class RecordingAnalyzer:
    """A stand-in for ``lineage.engine.analyze_definition`` that records what it was given and emits one READS_FROM edge per ``spark.table('x')`` in the code."""

    version = "test-1"

    def __init__(self) -> None:
        self.calls: list[M.DefinitionFact] = []

    def __call__(self, defn: M.DefinitionFact) -> Facts:
        import re

        self.calls.append(defn)
        code = " ".join(str(c.get("source", "")) for c in (defn.payload or {}).get("cells", []))
        out = Facts()
        for name in re.findall(r"spark\.table\(['\"](\w+)['\"]\)", code):
            out.edges.append(EdgeFact(defn.ref, AssetRef.make("table", None, name, {"workspace_id": defn.context["workspace_id"], "lakehouse_id": (defn.context.get("default_lakehouse") or {}).get("id", "")}),
                                      "READS_FROM", [Evidence("static", "test-analyzer", "derived", location={"cell": 1})]))
        return out


@pytest.fixture()
def analyzer(disc):
    a = RecordingAnalyzer()
    disc.engine.analyzer_loader = lambda: (a, a.version)
    return a


def test_definitions_are_analysed_and_unchanged_ones_are_skipped(disc, analyzer):
    disc.lakehouse()
    disc.write("people", people_table())
    nb = disc.notebook("etl", "df = spark.table('people')\n")
    first = disc.scan()
    assert first["status"] == "succeeded" and first["stats"]["apply"]["analyzed"] == 1 and first["stats"]["facts"]["definitions"] == 1
    assert len(analyzer.calls) == 1 and analyzer.calls[0].kind == "notebook" and analyzer.calls[0].context["item_id"] == nb["id"]
    assert analyzer.calls[0].context["default_lakehouse"]["name"] == "lh1" and analyzer.calls[0].etag == nb["etag"]
    edge = disc.edges(type="READS_FROM")
    assert len(edge) == 1 and edge[0]["status"] == "active"
    assert disc.asset("etl", "notebook")["id"] == edge[0]["src_id"] and disc.asset("people", "table")["id"] == edge[0]["dst_id"]
    again = disc.scan()                                                                  # same etag, same analyzer version: skipped, but kept alive through the sweep
    assert again["stats"]["apply"]["analysis_skipped"] == 1 and len(analyzer.calls) == 1
    assert disc.edges(type="READS_FROM")[0]["status"] == "active"
    r = disc.intel.put(f"/v1/items/{nb['id']}", json={"definition": {"cells": [{"cell_type": "code", "source": "df = spark.table('orders')"}], "language": "python"}},
                       headers={**disc.admin, "If-Match": nb["etag"]})
    assert r.status_code == 200, r.text
    changed = disc.scan(mode="incremental")
    assert changed["stats"]["apply"]["analyzed"] == 1 and len(analyzer.calls) == 2
    old = [e for e in disc.edges(type="READS_FROM") if e["dst_id"] == disc.asset("people", "table")["id"]]
    assert old[0]["status"] == "removed"                                                  # an edge the code no longer produces is retired by the re-analysis


def test_incremental_scan_does_not_report_unchanged_items_or_tables_again(disc, analyzer):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.notebook("etl", "df = spark.table('people')")
    disc.scan()
    inc = disc.scan(mode="incremental")
    assert inc["stats"]["counters"].get("tables_unchanged") == 1 and inc["stats"]["facts"]["definitions"] == 0
    assert inc["stats"]["apply"]["assets_created"] == 0 and len(analyzer.calls) == 1


def test_analyzer_missing_is_a_gap_not_a_crash(disc):
    disc.lakehouse()
    disc.notebook("etl", "df = spark.table('people')")

    def missing() -> Any:
        raise ModuleNotFoundError("no analyzers", name="olcf.intelligence.lineage.engine")

    disc.engine.analyzer_loader = missing
    row = disc.scan()
    assert row["status"] == "partial" or row["stats"]["apply"]["errors"] >= 1
    assert any("analyzers are not installed" in g["detail"] for g in disc.gaps())


def test_real_analyzers_find_lineage_when_installed(disc):
    pytest.importorskip("olcf.intelligence.lineage.engine")
    disc.lakehouse()
    disc.write("people", people_table())
    disc.notebook("etl", "df = spark.table('people')\ndf.write.mode('overwrite').saveAsTable('people_copy')\n")
    row = disc.scan()
    assert row["status"] in ("succeeded", "partial")
    types = {e["type"] for e in disc.edges()}
    assert "READS_FROM" in types and "WRITES_TO" in types


# ------------------------------------------------------------------------------------------------ opt-in value profiling and files
def test_value_profiling_is_refused_without_a_sampling_policy(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"value_profiling": True}})
    row = disc.scan()
    assert row["status"] == "succeeded"                                                  # "sampling is off" is information, not a failure
    gap = next(g for g in disc.gaps() if g["kind"] == "sampling_disabled")
    assert "no sampling policy" in gap["detail"] and "sampling-policies" in gap["remediation"]
    assert "value_profile" not in disc.asset("people", "table")["attrs"].get("profile", {})


def test_value_profiling_with_a_policy_stores_aggregates_and_suggestions_only(disc):
    disc.lakehouse()
    disc.write("contacts", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "note": ["write me at a1@corp.example", "b2@corp.example", "c3@corp.example"],
                                     "ref": ["one", "two", "three"]}))
    disc.d.put_policy(SYSTEM_PRINCIPAL, scope_type="global", scope_id=None, max_rows=50, masking="partial", exclude_classified=True)
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"value_profiling": True}})
    row = disc.scan()
    assert row["status"] == "succeeded"
    table = disc.asset("contacts", "table")["attrs"]
    assert table["profile"]["value_profile"]["rows"] == 50
    cols = {c["name"]: c for c in table["columns"]}
    assert cols["ref"]["value_profile"]["distinct"] == 3 and cols["ref"]["value_profile"]["non_null"] == 3
    note = disc.asset("note", "column")["attrs"]["suggested_classifications"]
    assert any(s["classification"] == "email" and "pattern" in " ".join(s["evidences"]) for s in note)
    blob = dump_graph(disc)
    assert "corp.example" not in blob and "three" not in blob


def test_listed_file_paths_are_reported_and_nothing_else(disc):
    disc.lakehouse()
    lh = disc.ctx.service("storage").get_lakehouse(disc.lh["id"])
    storage = disc.ctx.service("storage")
    storage.write_bytes(lh, "Files/raw/a.csv", b"id\n1\n")
    storage.write_bytes(lh, "Files/raw/b.csv", b"id\n2\n")
    storage.write_bytes(lh, "Files/private/secret.csv", b"id\n3\n")
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"files": [{"lakehouse": "lh1", "path": "Files/raw", "recursive": True}]}})
    disc.scan()
    names = sorted(a["name"] for a in disc.assets(type="file"))
    assert names == ["Files/raw/a.csv", "Files/raw/b.csv"]
    with pytest.raises(Exception, match="below 'Files/'"):
        disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"files": [{"lakehouse": "lh1", "path": "Tables/x"}]}})


# ------------------------------------------------------------------------------------------------ connectivity and configuration
def test_connectivity_steps_of_the_local_source(disc):
    disc.lakehouse()
    out = disc.d.test_source(SYSTEM_PRINCIPAL, "local")
    assert out["ok"] is True and [s["name"] for s in out["steps"]] == ["platform services", "workspaces", "lakehouse storage", "catalog"]
    assert all(s["ok"] for s in out["steps"]) and all(s["duration_ms"] >= 0 for s in out["steps"])


def test_connectivity_failure_stops_at_the_failing_step_with_remediation(disc):
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"workspace_ids": ["nope"]}})
    out = disc.d.test_source(SYSTEM_PRINCIPAL, "local")
    assert out["ok"] is False and out["failed_step"] == "workspaces"
    steps = {s["name"]: s for s in out["steps"]}
    assert steps["platform services"]["ok"] and not steps["workspaces"]["ok"] and "nope" in steps["workspaces"]["detail"] and steps["workspaces"]["remediation"]
    assert steps["lakehouse storage"]["skipped"] and steps["catalog"]["skipped"] and steps["lakehouse storage"]["ok"] is False


@pytest.mark.parametrize("config, message", [({"bogus": 1}, "unknown setting"), ({"workspace_ids": "x"}, "list"), ({"value_profiling": "yes"}, "true or false"),
                                             ({"max_tables_per_lakehouse": 0}, "between 1"), ({"files": [{"lakehouse": "a", "path": "Files/../x"}]}, "'..'")])
def test_invalid_local_configuration_is_rejected_with_a_field_message(disc, config, message):
    with pytest.raises(Exception, match=message):
        disc.d.sources.create(SYSTEM_PRINCIPAL, name="bad", kind="local", config=config)
