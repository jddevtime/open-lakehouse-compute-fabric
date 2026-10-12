# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""OLCF report (dashboard) analyzer: tiles, their tables and columns, filters, aliases and the gaps of broken tiles."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from typing import Any

from olcf.intelligence.lineage.base import LineageContext
from olcf.intelligence.lineage.report_olcf import MAX_TILES, analyze_report_olcf
from olcf.intelligence.model import AssetRef, Facts
from olcf.sample import reports as sample_reports
from tests.intelligence.lineage.support import (
    asset_labels,
    edges,
    gap_kinds,
    gap_text,
    gaps,
)

CTX: dict[str, Any] = {"item_id": "itm_rep", "item_name": "Rep", "default_lakehouse": {"name": "lh", "id": "itm_lh"}}


def tile(tid: str, sql: str | None, **kw: Any) -> dict[str, Any]:
    """A tile dict (title = upper-cased id, DuckDB engine)."""
    t: dict[str, Any] = {"id": tid, "title": tid.upper(), "engine": "duckdb", **kw}
    if sql is not None:
        t["sql"] = sql
    return t


def run(defn: Any, **c: Any) -> Facts:
    """Analyse a report definition in the standard test context."""
    return analyze_report_olcf(defn, context={**CTX, **c})


def test_sample_report_exact_shape() -> None:
    f = analyze_report_olcf(sample_reports.report_definition("itm_lh"), context={**CTX, "item_name": "Retail Sales Overview", "default_lakehouse": {"name": "retail_lakehouse", "id": "itm_lh"}})
    assert len(f.assets) == 11 and len(f.edges) == 50 and not f.gaps
    assert Counter(e.type for e in f.edges) == {"READS_FROM": 27, "DISPLAYS": 23}
    assert asset_labels(f, "dashboard") == {"dashboard:Retail Sales Overview"}
    assert len(asset_labels(f, "visual")) == 10
    # the report depends on exactly the four tables its tiles and the category filter read
    assert {e for e in edges(f, "READS_FROM") if e.startswith("READS_FROM dashboard:")} == {
        "READS_FROM dashboard:Retail Sales Overview -> table:retail_lakehouse.dim_date [derived]",
        "READS_FROM dashboard:Retail Sales Overview -> table:retail_lakehouse.dim_product [derived]",
        "READS_FROM dashboard:Retail Sales Overview -> table:retail_lakehouse.dim_store [derived]",
        "READS_FROM dashboard:Retail Sales Overview -> table:retail_lakehouse.fact_sales [derived]",
    }
    shown = edges(f, "DISPLAYS")
    assert "DISPLAYS visual:Revenue -> col:retail_lakehouse.fact_sales.net_amount [derived]" in shown
    assert "DISPLAYS visual:Revenue by category -> col:retail_lakehouse.dim_product.category [derived]" in shown
    assert "DISPLAYS visual:Customers -> col:retail_lakehouse.fact_sales.customer_id [derived]" in shown


def test_sample_report_bind_parameters_are_not_sources() -> None:
    f = analyze_report_olcf(sample_reports.report_definition("itm_lh"), context={**CTX, "default_lakehouse": {"name": "retail_lakehouse", "id": "itm_lh"}})
    for e in f.edges:
        if e.dst.type != "column":
            assert (e.dst.name or "").startswith("retail_lakehouse.dim_") or e.dst.name == "retail_lakehouse.fact_sales"
    assert not any(a.ref.type == "external" for a in f.assets)


def test_sample_report_aliases() -> None:
    f = analyze_report_olcf(sample_reports.report_definition("itm_lh"), context=CTX)
    by_type = {a.ref.type: dict(a.ref.alias_map) for a in f.assets if a.ref.type == "dashboard"}
    assert by_type["dashboard"] == {"olcf_item": "itm_rep"}
    visual_sources = sorted(a.ref.alias_map["source"] for a in f.assets if a.ref.type == "visual")
    assert visual_sources[0].startswith("olcf:itm_rep/tile/") and len(set(visual_sources)) == 10
    for a in f.assets:
        if a.ref.type == "visual":
            assert a.parent is not None and a.parent.type == "dashboard"


def test_tile_asset_alias_uses_source_id_when_known() -> None:
    f = run({"tiles": [tile("a", "SELECT x FROM t")]}, source_id="src1")
    assert [dict(a.ref.alias_map) for a in f.assets] == [{"olcf_item": "itm_rep"}, {"source": "src1:itm_rep/tile/a"}]


def test_tile_asset_without_item_id_has_no_alias_but_a_stable_scope() -> None:
    f = run({"title": "T", "tiles": [tile("a", "SELECT x FROM t")]}, item_id=None, item_name=None)
    visual = next(a for a in f.assets if a.ref.type == "visual")
    assert dict(visual.ref.alias_map) == {} and dict(visual.ref.scope_map) == {"report": "T", "tile": "a"}


def test_column_edges_and_output_names() -> None:
    f = run({"tiles": [tile("a", "SELECT a, a AS a2, upper(b) u, b + c AS bc FROM t1 WHERE x > :p")]})
    assert edges(f, "DISPLAYS") == {
        "DISPLAYS visual:A -> col:t1.a [derived]",
        "DISPLAYS visual:A -> col:t1.b [derived]",
        "DISPLAYS visual:A -> col:t1.c [derived]",
    }
    shown = {(e.dst.name or "").lower(): e.attrs.get("as") for e in f.edges if e.type == "DISPLAYS"}
    assert shown == {"a": "a, a2", "b": "u, bc", "c": "bc"}
    assert not f.gaps


def test_multiple_tiles_are_independent() -> None:
    f = run({"tiles": [tile("a", "SELECT a FROM t1"), tile("b", "SELECT b FROM t2")]})
    assert edges(f, "READS_FROM") == {
        "READS_FROM visual:A -> table:t1 [derived]",
        "READS_FROM visual:B -> table:t2 [derived]",
        "READS_FROM dashboard:Rep -> table:t1 [derived]",
        "READS_FROM dashboard:Rep -> table:t2 [derived]",
    }


def test_mixed_tiles_broken_ones_do_not_stop_the_rest() -> None:
    f = run({"tiles": [tile("a", "SELECT a, b + c AS bc FROM t1"), tile("b", "SELECT * FROM t2"), tile("c", ""), {"id": "d", "title": "D"}, tile("e", "SELEC broken"),
                       tile("f", "SELECT x FROM lh.t3 WHERE y = :p", engine="spark"), tile("g", "SELECT 1 AS one")]})
    assert edges(f, "DISPLAYS") == {
        "DISPLAYS visual:A -> col:t1.a [derived]",
        "DISPLAYS visual:A -> col:t1.b [derived]",
        "DISPLAYS visual:A -> col:t1.c [derived]",
        "DISPLAYS visual:F -> col:lh.t3.x [derived]",
    }
    assert {e for e in edges(f, "READS_FROM") if e.startswith("READS_FROM dashboard")} == {
        "READS_FROM dashboard:Rep -> table:t1 [derived]", "READS_FROM dashboard:Rep -> table:t2 [derived]", "READS_FROM dashboard:Rep -> table:lh.t3 [derived]",
    }
    assert sorted(gaps(f)) == sorted([("unsupported_construct", "t2"), ("not_inspected", "Rep/C"), ("not_inspected", "Rep/D"), ("parse_error", "SELEC broken")])
    assert {g.location.get("tile") for g in f.gaps} == {"b", "c", "d", "e"}
    assert len(asset_labels(f, "visual")) == 7  # every tile is an asset, even the broken ones


def test_tile_without_sql_is_not_inspected() -> None:
    for sql in (None, "", "   \n", 5):
        t = tile("a", None)
        if sql is not None:
            t["sql"] = sql
        f = run({"tiles": [t]})
        assert gaps(f) == [("not_inspected", "Rep/A")], sql
        assert gap_text(f).endswith("the tile has no SQL")
        assert not f.edges


def test_unparseable_tile_sql_is_a_parse_error_with_the_tile_location() -> None:
    for sql in ("SELEC broken", "SELECT FROM", "totally not sql", "SELECT a FROM"):
        f = run({"tiles": [tile("a", sql)]})
        assert gap_kinds(f) == {"parse_error"}, sql
        assert f.gaps[0].location["tile"] == "a" and not f.edges


def test_spark_engine_tile_uses_spark_dialect() -> None:
    sql = "SELECT `a` FROM `lh`.`t`"
    f = run({"tiles": [tile("a", sql, engine="spark")]})
    assert "READS_FROM visual:A -> table:lh.t [derived]" in edges(f)
    assert not f.gaps
    # the same text is not valid DuckDB
    g = run({"tiles": [tile("a", sql, engine="duckdb")]})
    assert "parse_error" in gap_kinds(g)


def test_engine_value_is_case_insensitive_and_unknown_means_duckdb() -> None:
    assert "READS_FROM visual:A -> table:lh.t [derived]" in edges(run({"tiles": [tile("a", "SELECT `a` FROM `lh`.`t`", engine="SPARK")]}))
    f = run({"tiles": [tile("a", "SELECT a FROM t", engine="something")]})
    assert edges(f, "DISPLAYS") == {"DISPLAYS visual:A -> col:t.a [derived]"}


def test_tile_lakehouse_id_selects_the_lakehouse() -> None:
    own = run({"tiles": [tile("a", "SELECT a FROM t1")]})
    other = run({"tiles": [tile("a", "SELECT a FROM t1", lakehouse_id="itm_other")]})
    same = run({"tiles": [tile("a", "SELECT a FROM t1", lakehouse_id="itm_lh")]})
    dst = lambda f: next(e.dst for e in f.edges if e.type == "READS_FROM" and e.src.type == "visual")  # noqa: E731
    assert dict(dst(own).scope_map) == {"lakehouse": "lh", "lakehouse_id": "itm_lh"}
    assert dict(dst(other).scope_map) == {"lakehouse_id": "itm_other"}
    assert dst(same) == dst(own)
    assert dst(own) != dst(other)


def test_non_olcf_lakehouse_id_is_ignored() -> None:
    f = run({"tiles": [tile("a", "SELECT a FROM t1", lakehouse_id="not-an-item-id")]})
    dst = next(e.dst for e in f.edges if e.type == "READS_FROM")
    assert dict(dst.scope_map) == {"lakehouse": "lh", "lakehouse_id": "itm_lh"}


def test_filter_options_sql_is_a_report_dependency() -> None:
    f = run({"tiles": [tile("a", "SELECT a FROM t1")],
             "filters": [{"name": "f", "options_sql": "SELECT DISTINCT k FROM dim", "options_lakehouse_id": "itm_other"}, {"name": "g"}, 5, {"name": "h", "options_sql": "  "}]})
    assert edges(f, "READS_FROM") == {
        "READS_FROM visual:A -> table:t1 [derived]",
        "READS_FROM dashboard:Rep -> table:t1 [derived]",
        "READS_FROM dashboard:Rep -> table:dim [derived]",
    }
    dim = next(e.dst for e in f.edges if e.dst.name == "dim")
    assert dict(dim.scope_map) == {"lakehouse_id": "itm_other"}
    assert not f.gaps


def test_filter_with_broken_options_sql_gives_a_gap_naming_the_filter() -> None:
    f = run({"tiles": [], "filters": [{"name": "region", "options_sql": "SELEC broken"}]})
    assert gaps(f) == [("parse_error", "SELEC broken")]
    assert f.gaps[0].location["filter"] == "region"


def test_report_asset_attrs() -> None:
    f = run({"title": "X", "tiles": [tile("a", "SELECT a FROM t")], "filters": [{"name": "f"}, {"name": "g"}]})
    report = next(a for a in f.assets if a.ref.type == "dashboard")
    assert report.name == "Rep" and report.attrs == {"tile_count": 1, "filter_count": 2}
    visual = next(a for a in f.assets if a.ref.type == "visual")
    assert visual.attrs == {"tile_id": "a", "tile_type": None, "engine": "duckdb"} and visual.path == "Rep / A"


def test_title_falls_back_to_definition_title() -> None:
    f = analyze_report_olcf({"title": "From defn", "tiles": []}, context={"item_id": "itm_rep"})
    assert asset_labels(f) == {"dashboard:From defn"}
    g = analyze_report_olcf({"tiles": []}, context={})
    assert asset_labels(g) == {"dashboard:report"}


def test_supplied_process_asset_is_the_report() -> None:
    proc = AssetRef.make("dashboard", {"olcf_item": "itm_rep"}, "Given")
    f = analyze_report_olcf({"tiles": [tile("a", "SELECT a FROM t")]}, context=replace(LineageContext.coerce(CTX), process=proc))
    assert {e.src for e in f.edges if e.src.type == "dashboard"} == {proc}


def test_duplicate_tile_ids_get_distinct_assets_and_a_gap() -> None:
    f = run({"tiles": [tile("a", "SELECT x FROM t1", title="First"), tile("a", "SELECT y FROM t9", title="Second"), tile("a", "SELECT z FROM t7", title="Third")]}, item_id="itm_r")
    sources = sorted(a.ref.alias_map["source"] for a in f.assets if a.ref.type == "visual")
    assert sources == ["olcf:itm_r/tile/a", "olcf:itm_r/tile/a#2", "olcf:itm_r/tile/a#3"]
    assert edges(f, "DISPLAYS") == {
        "DISPLAYS visual:First -> col:t1.x [derived]", "DISPLAYS visual:Second -> col:t9.y [derived]", "DISPLAYS visual:Third -> col:t7.z [derived]",
    }
    assert gaps(f) == [("parse_error", "Rep/a"), ("parse_error", "Rep/a")]
    assert "share the id 'a'" in gap_text(f)


def test_tile_without_id_is_named_by_position() -> None:
    f = run({"tiles": [{"title": "Anon", "sql": "SELECT a FROM t"}]})
    visual = next(a for a in f.assets if a.ref.type == "visual")
    assert visual.attrs["tile_id"] == "tile0" and visual.ref.alias_map["source"] == "olcf:itm_rep/tile/tile0"


def test_malformed_definitions() -> None:
    assert gaps(run([])) == [("parse_error", "Rep")]
    assert gaps(run("x")) == [("parse_error", "Rep")]
    assert gaps(run(None)) == [("parse_error", "Rep")]
    assert gaps(run({"tiles": "x"})) == [("parse_error", "Rep")]
    f = run({"tiles": [5, None, "x"]})
    assert gaps(f) == [("parse_error", "Rep/tile 0"), ("parse_error", "Rep/tile 1"), ("parse_error", "Rep/tile 2")]
    assert asset_labels(f) == {"dashboard:Rep"}
    empty = run({"title": "R", "tiles": []})
    assert not empty.edges and not empty.gaps and asset_labels(empty) == {"dashboard:Rep"}
    nulls = run({"tiles": None, "filters": None})
    assert not nulls.gaps and not nulls.edges


def test_tile_cap_produces_a_gap() -> None:
    f = run({"tiles": [tile(f"t{i}", "SELECT a FROM t") for i in range(MAX_TILES + 5)]})
    assert ("unsupported_construct", "Rep") in gaps(f)
    assert sum(1 for a in f.assets if a.ref.type == "visual") == MAX_TILES


def test_evidence_is_static_with_the_tile_sql() -> None:
    f = run({"tiles": [tile("a", "SELECT a FROM t1")]})
    for e in f.edges:
        assert e.evidence and e.evidence[0].kind == "static"
        assert e.evidence[0].snippet == "SELECT a FROM t1"
        assert e.evidence[0].location["tile"] == "a"


def test_is_deterministic() -> None:
    d = sample_reports.report_definition("itm_lh")
    assert run(d).edges == run(d).edges
