# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Profiling: statistics from the Delta log only (never a text min / max, never a sensitive column's), honest gaps for what the log does not say, and value profiling that
returns aggregates and suggestions but no cell value."""

from __future__ import annotations

import json
from pathlib import Path

import deltalake
import pyarrow as pa
import pytest

from olcf.intelligence.discovery import profiling

COLS = [{"name": "id", "type": "bigint"}, {"name": "email", "type": "string"}, {"name": "amount", "type": "double"}, {"name": "city", "type": "string"},
        {"name": "salary", "type": "double"}, {"name": "created", "type": "timestamp"}]


def make(path: Path, rows: int = 5, **kw) -> deltalake.DeltaTable:
    import datetime as dt

    t = pa.table({"id": pa.array(range(rows), pa.int64()), "email": pa.array([f"u{i}@example.com" if i % 2 == 0 else None for i in range(rows)]),
                  "amount": pa.array([float(i) for i in range(rows)]), "city": pa.array(["Oslo"] * rows), "salary": pa.array([1000.0 + i for i in range(rows)]),
                  "created": pa.array([dt.datetime(2025, 1, 1 + i) for i in range(rows)], pa.timestamp("us"))})
    deltalake.write_deltalake(str(path), t, **kw)
    return deltalake.DeltaTable(str(path))


def test_metadata_profile_comes_from_the_log(tmp_path):
    table = make(tmp_path / "t", rows=5)
    profile, gaps = profiling.profile_delta_log(table, COLS, subject="t")
    assert gaps == [] and profile["complete"] is True and profile["source"] == "delta_log"
    assert profile["row_count"] == 5 and profile["num_files"] == 1 and profile["version"] == 0 and profile["size_bytes"] > 0 and profile["last_modified"].endswith("Z")
    c = profile["columns"]
    assert c["id"] == {"null_count": 0, "min": 0, "max": 4} and c["amount"]["max"] == 4.0
    assert c["email"]["null_count"] == 2 and c["email"]["min"] is None and c["email"]["max"] is None          # text: null count only, never a value
    assert c["city"]["min"] is None                                                                           # text
    assert c["salary"]["null_count"] == 0 and c["salary"]["min"] is None                                      # sensitive by name: no range kept
    assert c["created"]["min"].startswith("2025-01-01") and c["created"]["max"].startswith("2025-01-05")
    assert "example.com" not in json.dumps(profile) and "Oslo" not in json.dumps(profile)


def test_totals_span_every_file_and_version_follows_the_table(tmp_path):
    make(tmp_path / "t", rows=4)
    deltalake.write_deltalake(str(tmp_path / "t"), make(tmp_path / "u", rows=3).to_pyarrow_table(), mode="append")
    table = deltalake.DeltaTable(str(tmp_path / "t"))
    profile, gaps = profiling.profile_delta_log(table, COLS, subject="t")
    assert profile["num_files"] == 2 and profile["row_count"] == 7 and profile["version"] == 1 and gaps == []
    assert profile["columns"]["id"]["min"] == 0 and profile["columns"]["id"]["max"] == 3


def test_columns_beyond_the_statistics_limit_are_a_gap_not_a_zero(tmp_path):
    table = make(tmp_path / "t")
    wide = COLS + [{"name": "extra", "type": "string"}]
    profile, gaps = profiling.profile_delta_log(table, wide, subject="t", stats_columns=3)
    assert profile["complete"] is False and profile["columns"]["extra"]["null_count"] is None
    assert any(g.kind == "not_inspected" and "beyond the first 3" in g.detail for g in gaps)
    assert all(g.source == "discovery:local" and g.subject == "t" for g in gaps)


def test_missing_statistics_are_reported(tmp_path):
    table = make(tmp_path / "t")
    ghost = COLS + [{"name": "never_written", "type": "string"}]
    profile, gaps = profiling.profile_delta_log(table, ghost, subject="t")
    assert profile["columns"]["never_written"]["null_count"] is None and profile["row_count"] == 5
    assert any("incomplete" in g.detail and "never_written" in g.detail for g in gaps)


def test_an_unreadable_log_is_a_gap_with_unknown_numbers():
    class Broken:
        def get_add_actions(self, flatten: bool = True):
            raise OSError("boom")

        def version(self):
            raise RuntimeError("no version")

    profile, gaps = profiling.profile_delta_log(Broken(), COLS, subject="broken", source="discovery:x", location={"path": "/p"})
    assert profile["row_count"] is None and profile["version"] is None and profile["complete"] is False and profile["columns"] == {}
    assert len(gaps) == 1 and gaps[0].kind == "not_inspected" and gaps[0].source == "discovery:x" and gaps[0].location == {"path": "/p"} and "OSError" in gaps[0].detail


def test_an_empty_table_has_zero_files_and_no_gaps(tmp_path):
    deltalake.write_deltalake(str(tmp_path / "e"), pa.table({"id": pa.array([], pa.int64())}))
    profile, gaps = profiling.profile_delta_log(deltalake.DeltaTable(str(tmp_path / "e")), [{"name": "id", "type": "bigint"}], subject="e")
    assert profile["num_files"] == 0 and profile["size_bytes"] in (0, None) and gaps == []


@pytest.mark.parametrize("value, expected", [(None, None), (True, True), (3, 3), (2**60, str(2**60)), (float("nan"), None), (float("inf"), None), (1.5, 1.5)])
def test_json_scalars(value, expected):
    assert profiling._json_scalar(value) == expected


def test_json_scalar_dates_and_decimals():
    import datetime as dt
    import decimal

    assert profiling._json_scalar(dt.date(2025, 1, 2)) == "2025-01-02" and profiling._json_scalar(dt.datetime(2025, 1, 2, 3, 4)) == "2025-01-02T03:04:00"
    assert profiling._json_scalar(decimal.Decimal("1.50")) == 1.5 and profiling._json_scalar(decimal.Decimal("NaN")) is None and profiling._json_scalar(b"x") == "b'x'"


# ------------------------------------------------------------------------------------------------ values
def test_value_profile_returns_aggregates_and_pattern_suggestions_only():
    emails = [f"person{i}@corp.example" for i in range(8)] + [None, None]
    t = pa.table({"contact": pa.array(emails), "n": pa.array(list(range(10)), pa.int64()), "same": pa.array(["x"] * 10)})
    out = profiling.profile_values(t, [{"name": "n", "type": "bigint"}])
    assert out["contact"]["rows"] == 10 and out["contact"]["non_null"] == 8 and out["contact"]["distinct"] == 8 and out["contact"]["distinct_capped"] is False
    assert [s["classification"] for s in out["contact"]["suggested_classifications"]] == ["email"]
    assert out["same"]["distinct"] == 1 and out["n"]["suggested_classifications"] == [] and out["n"]["distinct"] == 10
    assert "corp.example" not in json.dumps(out)


def test_name_only_suggestions_are_not_part_of_the_value_profile():
    t = pa.table({"password": pa.array(["a", "b", "c"])})
    assert profiling.profile_values(t)["password"]["suggested_classifications"] == []                         # name evidence belongs to detect.annotate_facts, not to sampling


def test_distinct_counts_are_capped(monkeypatch):
    monkeypatch.setattr(profiling, "DISTINCT_CAP", 5)
    out = profiling.profile_values(pa.table({"v": pa.array(range(20), pa.int64())}))
    assert out["v"]["distinct"] == 5 and out["v"]["distinct_capped"] is True


def test_complex_values_do_not_break_the_profile():
    t = pa.table({"tags": pa.array([["a", "b"], ["a"], None]), "obj": pa.array([{"x": 1}, {"x": 2}, {"x": 1}])})
    out = profiling.profile_values(t)
    assert out["tags"]["non_null"] == 2 and out["tags"]["distinct"] == 2 and out["obj"]["distinct"] == 2


def test_merge_value_profile_is_a_copy_and_keeps_the_stronger_suggestion():
    cols = [{"name": "contact", "type": "string", "suggested_classifications": [{"classification": "email", "confidence": 0.99, "evidence": "name"},
                                                                                {"classification": "phone", "confidence": 0.5, "evidence": "name"}]}, {"name": "other", "type": "int"}]
    prof = {"contact": {"rows": 4, "non_null": 4, "distinct": 4, "distinct_capped": False,
                        "suggested_classifications": [{"classification": "email", "confidence": 0.8, "evidence": "pattern"}, {"classification": "iban", "confidence": 0.9, "evidence": "pattern"}]}}
    merged = profiling.merge_value_profile(cols, prof)
    assert merged[0]["value_profile"] == {"rows": 4, "non_null": 4, "distinct": 4, "distinct_capped": False} and "value_profile" not in merged[1]
    by = {s["classification"]: s for s in merged[0]["suggested_classifications"]}
    assert by["email"]["confidence"] == 0.99 and set(by) == {"email", "phone", "iban"} and [s["classification"] for s in merged[0]["suggested_classifications"]][0] == "email"
    assert "value_profile" not in cols[0] and len(cols[0]["suggested_classifications"]) == 2
