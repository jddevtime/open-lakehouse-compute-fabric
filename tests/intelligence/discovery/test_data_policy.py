# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The shared data-read policy service (review findings F01, F02, F03).

* F01 - a column sample is governed by its *parent table's* policy (a broader allow never defeats a table-level ban; stricter table limits / masking hold).
* F02 - automatic value profiling asks the same service: asset-level policies, approved governance classifications, fail-closed provider errors, zero storage reads when off.
* F03 - the projection is enforced on the declared-schema and the reader-schema paths alike: a column asset yields exactly its column.

Every "no" is proved with a reader spy: the storage reader must not even be opened.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pytest
from sqlalchemy import select, update

from olcf.intelligence.schema import assets
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table
from tests.intelligence.discovery.test_local_connector import dump_graph
from tests.intelligence.discovery.test_sampling import fabric_table, put, sample


class ReaderSpy:
    """Counts every open of a local table for data reading and records the column projections asked of the reader."""

    def __init__(self, tables: Any) -> None:
        self.opens = 0
        self.projections: list[Any] = []
        real = tables.scan_source

        def scan_source(lh: Any, name: str) -> Any:
            self.opens += 1
            dataset = real(lh, name)
            spy = self

            class _Dataset:
                schema = dataset.schema

                def scanner(self, columns: Any = None, **kw: Any) -> Any:
                    spy.projections.append(None if columns is None else list(columns))
                    return dataset.scanner(columns=columns, **kw)

            return _Dataset()

        self._restore = (tables, real)
        tables.scan_source = scan_source

    def close(self) -> None:
        self._restore[0].scan_source = self._restore[1]


@pytest.fixture()
def world(disc):
    disc.lakehouse()
    disc.write("people", people_table(6))
    disc.scan()
    disc.people = disc.asset("people", "table")
    disc.wsadmin = disc.intel.user("wa_dp", "WorkspaceAdmin", workspace_id=disc.ws["id"])
    return disc


@pytest.fixture()
def spy(world):
    s = ReaderSpy(world.ctx.service("storage").tables)
    yield s
    s.close()


def column(world: Any, name: str, table: dict[str, Any] | None = None) -> dict[str, Any]:
    parent = (table or world.people)["id"]
    return next(a for a in world.assets(type="column", name=name) if a["parent_id"] == parent)


def set_attrs(world: Any, asset_id: str, **changes: Any) -> None:
    """Rewrite attributes of an asset row directly (simulates a scan that recorded no / partial schema)."""
    with world.ctx.db.begin() as c:
        row = c.execute(select(assets.c.attrs).where(assets.c.id == asset_id)).scalar_one()
        attrs = dict(row)
        for k, v in changes.items():
            if v is None:
                attrs.pop(k, None)
            else:
                attrs[k] = v
        c.execute(update(assets).where(assets.c.id == asset_id).values(attrs=attrs))


# ------------------------------------------------------------------------------------------------ F01: the table decides
@pytest.mark.parametrize("scope", ["global", "workspace", "source"])
def test_a_broader_allow_cannot_defeat_a_table_ban_through_a_column_request(world, spy, scope):
    ids = {"global": None, "workspace": world.ws["id"], "source": world.source()["id"]}
    put(world, scope_type=scope, scope_id=ids[scope], max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=0)
    assert sample(world, world.people).status_code == 403
    r = sample(world, column(world, "city"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "policy_denied" and r.json()["error"]["details"]["reason"] == "sampling_disabled"
    assert "Oslo" not in r.text
    assert spy.opens == 0                                                      # denied before the reader was invoked


def test_a_disabled_table_policy_also_denies_its_columns(world, spy):
    put(world, max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=5, enabled=False)
    assert sample(world, column(world, "city")).status_code == 403
    assert spy.opens == 0


def test_a_stricter_table_row_limit_and_masking_hold_for_its_columns(world):
    put(world, max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=2, masking="redact")
    out = sample(world, column(world, "city"), rows=5).json()
    assert out["row_count"] == 2 and out["max_rows"] == 2 and out["policy_scope"] == f"asset:{world.people['id']}"
    assert all(set(r) == {"city"} and r["city"] in ("***", None) for r in out["rows"]) and "Oslo" not in json.dumps(out)


def test_a_column_policy_may_narrow_but_never_broaden_the_table_policy(world, spy):
    city = column(world, "city")
    put(world, max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=city["id"], max_rows=0)               # the column is switched off, the table is not
    assert sample(world, city).status_code == 403
    ok = sample(world, world.people, rows=2)
    assert ok.status_code == 200
    put(world, scope_type="asset", scope_id=city["id"], max_rows=1, masking="redact")           # narrower row limit and stronger masking
    out = sample(world, city, rows=5).json()
    assert out["row_count"] == 1 and out["rows"] == [{"city": "***"}] and out["masking"] == "redact"
    # a column row that tries to be wider than the table row has no effect
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=2, masking="hash")
    put(world, scope_type="asset", scope_id=city["id"], max_rows=100, masking="none")
    wide = sample(world, city, rows=50).json()
    assert wide["row_count"] == 2 and wide["masking"] == "hash" and "Oslo" not in json.dumps(wide)
    # and it cannot re-enable a banned table
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=0)
    spy.opens = 0
    assert sample(world, city).status_code == 403 and spy.opens == 0


def test_the_effective_policy_endpoint_reports_the_tables_policy_for_a_column(world):
    put(world, max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=0)
    pol = world.api("GET", f"/assets/{column(world, 'city')['id']}/sampling-policy").json()
    assert pol["allowed"] is False and pol["scope"] == f"asset:{world.people['id']}"


def test_a_workspace_admin_gets_the_same_table_decision(world, spy):
    put(world, max_rows=5, masking="none")
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=0)
    assert sample(world, column(world, "city"), who=world.wsadmin).status_code == 403
    assert spy.opens == 0


# ------------------------------------------------------------------------------------------------ F03: projection on both schema paths
def test_without_recorded_columns_a_column_asset_still_yields_exactly_its_column(world, spy):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=None)
    out = sample(world, column(world, "city"), rows=3).json()
    assert [c["name"] for c in out["columns"]] == ["city"] and out["rows"][0] == {"city": "Oslo"}
    assert all(set(r) == {"city"} for r in out["rows"])
    assert spy.projections[-1] == ["city"]                                      # the reader was asked for that one column only


def test_empty_recorded_columns_behave_like_missing_ones(world):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=[])
    out = sample(world, column(world, "amount"), rows=2).json()
    assert [c["name"] for c in out["columns"]] == ["amount"]


def test_explicit_columns_parameter_is_honoured_without_recorded_columns(world, spy):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=None)
    out = sample(world, world.people, columns=["city", "id"], rows=2).json()
    assert sorted(c["name"] for c in out["columns"]) == ["city", "id"]
    assert sorted(spy.projections[-1]) == ["city", "id"]
    assert "email" not in spy.projections[-1] and "ssn" not in spy.projections[-1]       # sensitive columns are not even read


def test_a_requested_column_the_table_does_not_have_is_an_error_not_a_widening(world, spy):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=None)
    r = sample(world, world.people, columns=["no_such_column"])
    assert r.status_code == 400 and "no_such_column" in r.json()["error"]["message"]
    ghost = column(world, "city")
    with world.ctx.db.begin() as c:                                             # a column asset whose physical column is gone
        c.execute(update(assets).where(assets.c.id == ghost["id"]).values(name="vanished"))
    r2 = sample(world, ghost)
    assert r2.status_code == 400 and "vanished" in r2.json()["error"]["message"]
    assert not any("amount" in (p or []) for p in spy.projections)


def test_a_missing_physical_column_of_a_declared_schema_is_an_error_too(world):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=[{"name": "id", "type": "bigint"}, {"name": "city", "type": "string"}, {"name": "dropped", "type": "string"}])
    ghost = column(world, "city")
    with world.ctx.db.begin() as c:
        c.execute(update(assets).where(assets.c.id == ghost["id"]).values(name="dropped"))
    r = sample(world, ghost)
    assert r.status_code in (400, 422)                                           # the reader has no such column: explicit failure, never the whole table
    assert "Oslo" not in r.text and "user0@example.com" not in r.text


def test_a_classified_column_asset_is_refused_when_the_schema_is_unknown(world, spy):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=None)
    r = sample(world, column(world, "email"))
    assert r.status_code == 403 and r.json()["error"]["details"]["reason"] == "column_excluded"
    assert "user0@example.com" not in r.text


def test_a_governance_classified_unobvious_column_is_excluded_on_the_reader_schema(world, spy):
    put(world, max_rows=3, masking="none")
    set_attrs(world, world.people["id"], columns=None)
    world.d.sampling.add_classified_provider(lambda aid: {"City"})
    out = sample(world, world.people, rows=2).json()
    assert "city" not in {c["name"] for c in out["columns"]} and "city" in {x.lower() for x in out["excluded_columns"]}
    assert all("city" not in (p or []) for p in spy.projections)
    r = sample(world, column(world, "city"))
    assert r.status_code == 403 and r.json()["error"]["details"]["reason"] == "column_excluded"


def test_mixed_case_column_names_are_matched_and_returned_under_the_physical_name(world, spy):
    world.write("mixed", pa.table({"Id": pa.array([1, 2, 3], pa.int64()), "CityName": ["Oslo", "Rome", "Lima"], "Amount": [1.0, 2.0, 3.0]}))
    world.scan()
    mixed = world.asset("mixed", "table")
    put(world, max_rows=3, masking="none")
    col = column(world, "CityName", mixed)
    declared = sample(world, col).json()
    assert [c["name"] for c in declared["columns"]] == ["CityName"]
    set_attrs(world, mixed["id"], columns=None)
    undeclared = sample(world, col).json()
    assert [c["name"] for c in undeclared["columns"]] == ["CityName"] and undeclared["rows"][0] == {"CityName": "Oslo"}
    by_param = sample(world, mixed, columns=["cityname", "AMOUNT"]).json()
    assert sorted(c["name"] for c in by_param["columns"]) == ["Amount", "CityName"]


def test_the_projection_is_enforced_on_whatever_a_fabric_reader_returns(world):
    fab = fabric_table(world)
    put(world, max_rows=5, masking="none")
    world.d.sampling.fabric_reader = lambda source, identity, attrs, rows, columns: pa.table({"id": [1, 2], "note": ["a", "b"], "ssn": ["111-22-3333", "444-55-6666"]})
    out = sample(world, fab["asset"], columns=["note"]).json()                                  # declared schema: id, note - the reader also returned an undeclared sensitive column
    assert [c["name"] for c in out["columns"]] == ["note"] and "111-22-3333" not in json.dumps(out)
    whole = sample(world, fab["asset"]).json()
    assert [c["name"] for c in whole["columns"]] == ["id", "note"] and "ssn" in {x.lower() for x in whole["excluded_columns"]}
    assert "111-22-3333" not in json.dumps(whole)


def test_fabric_column_projection_without_a_recorded_schema(world):
    fab = fabric_table(world)
    put(world, max_rows=5, masking="none")
    set_attrs(world, fab["asset"]["id"], columns=None)
    world.d.sampling.fabric_reader = lambda source, identity, attrs, rows, columns: pa.table({"Id": [1, 2], "Note": ["a", "b"], "email": ["x@y.example", "z@y.example"]})
    out = sample(world, fab["asset"], columns=["note"]).json()
    assert [c["name"] for c in out["columns"]] == ["Note"] and "x@y.example" not in json.dumps(out)
    missing = sample(world, fab["asset"], columns=["nope"])
    assert missing.status_code == 400


# ------------------------------------------------------------------------------------------------ F02: profiling asks the same service
def enable_profiling(world: Any) -> None:
    world.d.sources.update(SYSTEM_PRINCIPAL, world.source()["id"], {"config": {"value_profiling": True}})


def test_a_rescanned_table_with_a_table_level_ban_causes_zero_storage_reads(world, spy):
    put(world, max_rows=50, masking="partial")                                         # a broad allow ...
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=0)             # ... and an explicit ban on this table
    enable_profiling(world)
    row = world.scan()
    assert row["status"] == "succeeded"
    assert spy.opens == 0                                                              # not a single data read
    assert any(g["kind"] == "sampling_disabled" and "people" in g["subject"] for g in world.gaps())
    assert "value_profile" not in (world.asset("people", "table")["attrs"].get("profile") or {})
    assert all("value_profile" not in c for c in world.asset("people", "table")["attrs"]["columns"])


def test_a_disabled_policy_and_a_zero_global_policy_read_nothing_for_a_new_table(world, spy):
    world.write("brand_new", pa.table({"k": pa.array([1, 2], pa.int64())}))
    put(world, max_rows=0)
    enable_profiling(world)
    world.scan()
    assert spy.opens == 0
    put(world, max_rows=10, enabled=False)
    world.scan()
    assert spy.opens == 0


def test_a_table_level_allow_turns_profiling_on_where_the_broad_policy_is_off(world, spy):
    put(world, max_rows=0)
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=4, masking="partial")
    enable_profiling(world)
    world.scan()
    assert spy.opens == 1
    assert world.asset("people", "table")["attrs"]["profile"]["value_profile"]["rows"] == 4


def test_approved_governance_classifications_exclude_unobviously_named_columns_from_profiling(world, spy):
    put(world, max_rows=20, masking="partial")
    world.d.sampling.add_classified_provider(lambda aid: {"amount"})                 # a name no heuristic would flag
    enable_profiling(world)
    world.scan()
    assert spy.projections and all("amount" not in (p or []) for p in spy.projections)
    cols = {c["name"]: c for c in world.asset("people", "table")["attrs"]["columns"]}
    assert "value_profile" not in cols["amount"] and "value_profile" in cols["city"]


def test_a_failing_classification_lookup_fails_closed_for_profiling(world, spy):
    put(world, max_rows=20, masking="partial")

    def boom(_: str) -> set[str]:
        raise RuntimeError("governance db down")

    world.d.sampling.add_classified_provider(boom)
    enable_profiling(world)
    row = world.scan()
    assert spy.opens == 0
    assert row["status"] in ("partial", "succeeded")
    gap = next(g for g in world.gaps() if "classification lookup failed" in g["detail"])
    assert gap["kind"] == "not_inspected" and "governance db down" not in json.dumps(world.gaps())
    assert "value_profile" not in (world.asset("people", "table")["attrs"].get("profile") or {})


def test_profiling_output_holds_aggregates_only_and_the_read_is_audited_without_values(world, spy):
    world.write("contacts", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "note": ["write me at a1@corp.example", "b2@corp.example", "c3@corp.example"], "ref": ["one", "two", "three"]}))
    put(world, max_rows=50, masking="partial")
    enable_profiling(world)
    world.scan()
    blob = dump_graph(world)
    assert "corp.example" not in blob and "three" not in blob
    contacts = world.asset("contacts", "table")
    records = [r for r in world.ctx.audit.query(action="intelligence.sample_read", limit=50) if (r.get("details") or {}).get("purpose") == "value_profiling"]
    assert records, "the authorised profiling read must be audited"
    rec = next(r for r in records if (r["details"] or {}).get("asset_name") == "contacts")
    assert rec["details"]["rows"] == 3 and rec["details"]["masking"] == "aggregate_only" and "ref" in rec["details"]["columns"]
    assert "corp.example" not in json.dumps(world.ctx.audit.query(limit=500), default=str)
    assert contacts["attrs"]["profile"]["value_profile"]["policy_scope"] == "global"


def test_profiling_and_sampling_agree_on_every_decision(world, spy):
    """Whatever the interactive sample may not read, profiling does not read either (same service, same answer)."""
    put(world, max_rows=10, masking="partial")
    put(world, scope_type="source", scope_id=world.source()["id"], max_rows=0)
    put(world, scope_type="asset", scope_id=world.people["id"], max_rows=7, masking="none")
    enable_profiling(world)
    world.scan()
    sampled = sample(world, world.people, rows=7)
    assert sampled.status_code == 200 and sampled.json()["policy_scope"] == f"asset:{world.people['id']}"
    assert world.asset("people", "table")["attrs"]["profile"]["value_profile"]["policy_scope"] == f"asset:{world.people['id']}"
