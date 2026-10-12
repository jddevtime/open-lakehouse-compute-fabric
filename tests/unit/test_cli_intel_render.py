# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The pure terminal renderers of OLCF Intelligence answers (``olcf.cli.intel_render``) on hand-built documents: tree shapes, cycles, diamonds, clipping, column
rows, origin chains, impact / staleness rows and connectivity-test lines."""

from __future__ import annotations

from olcf.cli import intel_render as R


def node(i: str, name: str, typ: str = "table", **kw):
    return {"id": i, "name": name, "type": typ, **kw}


def edge(a: str, b: str, conf: str = "derived", **kw):
    return {"from": a, "to": b, "confidence": conf, **kw}


def test_label_flags_placeholders_removed_assets_and_ids():
    assert R.label(node("a", "orders")) == "orders (table)"
    assert R.label(node("a", "orders", placeholder=True)) == "orders (table) [unresolved]"
    assert R.label(node("a", "orders", status="removed")) == "orders (table) [removed]"
    assert R.label(node("a", "orders"), ids=True) == "orders (table)  a"
    assert R.label(None, "ast_1") == "ast_1" and R.label(None) == "?"


def test_lineage_tree_both_directions_with_processes_and_gaps():
    doc = {"root": node("c", "clean", path="lh/clean"), "direction": "both",
           "nodes": [node("e", "etl", "notebook"), node("r", "raw"), node("m", "mart")],
           "edges": [edge("e", "c", "verified"), edge("r", "e"), edge("c", "m", "inferred")],
           "gaps": [{"kind": "dynamic_reference"}, {"kind": "dynamic_reference"}, {"kind": "permission_denied"}]}
    assert R.lineage_tree(doc) == [
        "clean (table)  lh/clean",
        "├── upstream (flows into it)",
        "│   └── <- etl (notebook)  [verified]",
        "│       └── <- raw (table)  [derived]",
        "└── downstream (it flows into)",
        "    └── -> mart (table)  [inferred]",
        "lineage is incomplete: 3 gap(s) (dynamic_reference x2, permission_denied x1); see `olcf intelligence gaps`",
    ]


def test_lineage_tree_single_direction_and_empty_sides():
    doc = {"root": node("c", "clean"), "direction": "down", "nodes": [], "edges": []}
    assert R.lineage_tree(doc) == ["clean (table)", "└── downstream (it flows into): none found"]
    up = {"root": node("c", "clean"), "direction": "up", "nodes": [node("r", "raw")], "edges": [edge("r", "c")]}
    assert R.lineage_tree(up) == ["clean (table)", "└── upstream (flows into it)", "    └── <- raw (table)  [derived]"]


def test_lineage_tree_marks_diamonds_cycles_and_virtual_edges_and_sorts_children():
    doc = {"root": node("a", "a"), "direction": "down",
           "nodes": [node("b", "b"), node("c", "c"), node("d", "d"), node("z", "z")],
           "edges": [edge("a", "c"), edge("a", "b"), edge("b", "d"), edge("c", "d"), edge("d", "z"), edge("d", "a"), edge("a", "z", "inferred", virtual=True)]}
    assert R.lineage_tree(doc) == [
        "a (table)",
        "└── downstream (it flows into)",
        "    ├── -> b (table)  [derived]",
        "    │   └── -> d (table)  [derived]",
        "    │       ├── -> a (table)  [derived] (cycle)",
        "    │       └── -> z (table)  [derived]",
        "    ├── -> c (table)  [derived]",
        "    │   └── -> d (table)  [derived] (see above)",
        "    └── -> z (table)  [inferred] (governance)",
    ]


def test_lineage_tree_style_callback_and_ids_and_clipping(monkeypatch):
    doc = {"root": node("a", "a"), "direction": "down", "nodes": [node("b", "b")], "edges": [edge("a", "b", "verified")]}
    assert R.lineage_tree(doc, style=lambda t, role: f"<{role}:{t}>", ids=True)[-1] == "    └── -> b (table)  b  [<verified:verified>]"
    monkeypatch.setattr(R, "MAX_TREE_LINES", 2)
    wide = {"root": node("a", "a"), "direction": "down", "nodes": [node(str(i), f"n{i}") for i in range(5)], "edges": [edge("a", str(i)) for i in range(5)], "truncated": True,
            "stats": {"max_nodes": 5}}
    out = R.lineage_tree(wide)
    assert sum("->" in ln for ln in out) == 2
    assert out[-2:] == ["(output cut at 2 lines; narrow it with --depth, --types or --min-confidence)",
                        "(the answer was cut at the server's node cap of 5; lower --depth or filter with --types)"]


def test_column_rows_qualify_columns_sort_upstream_first_and_name_the_process():
    doc = {"root": node("c1", "amount", "column", parent_id="t_clean"),
           "nodes": [node("c0", "amount", "column", parent_id="t_raw", direction="upstream"), node("c2", "total", "column", parent_id="t_mart", direction="downstream"),
                     node("p", "etl", "notebook")],
           "edges": [edge("c1", "c2", via="p", transformation="sum(amount)"), edge("c0", "c1", "inferred", via="p", transformation="amount * 1.0")]}
    rows = R.column_rows(doc, {"t_clean": "clean", "t_raw": "raw", "t_mart": "mart"})
    assert [(r["direction"], r["from"], r["to"], r["via"], r["transformation"], r["confidence"]) for r in rows] == [
        ("upstream", "raw.amount", "clean.amount", "etl", "amount * 1.0", "inferred"),
        ("downstream", "clean.amount", "mart.total", "etl", "sum(amount)", "derived")]
    bare = R.column_rows(doc)
    assert bare[0]["from"] == "amount" and bare[1]["to"] == "total"
    assert R.parent_ids(doc) == ["t_clean", "t_mart", "t_raw"]
    assert R.parent_ids({"root": node("c1", "amount", "column", parent_id="t"), "nodes": [node("t", "t")]}) == []


def test_origin_roots_and_chains_name_columns_with_their_tables():
    doc = {"asset": node("rep", "report", "report"),
           "nodes": [node("c0", "amount", "column", parent_id="t_raw"), node("c1", "amount", "column", parent_id="t_clean"), node("etl", "etl", "notebook")],
           "roots": [{"asset": node("c0", "amount", "column", parent_id="t_raw", workspace_id=None), "is_source": False, "path_count": 1, "best_confidence": "derived"}],
           "paths": [{"nodes": ["c0", "etl", "c1"], "confidence": "derived", "length": 1, "processes": ["etl"]}]}
    names = {"t_raw": "raw", "t_clean": "clean"}
    roots = R.origin_roots(doc, names)
    assert roots[0]["name"] == "raw.amount" and roots[0]["workspace_id"] == "(organisation)" and roots[0]["source"] == "no" and roots[0]["paths"] == 1
    assert R.origin_chains(doc, table_names=names) == ["raw.amount -> etl -> clean.amount  [derived, 1 hop(s), via etl]"]
    assert R.origin_chains(doc) == ["amount -> etl -> amount  [derived, 1 hop(s), via etl]"]
    assert R.qualified(node("x", "plain"), {}, None) == "plain" and R.qualified(None, {}) == "?"


def test_impact_rows_sorted_by_hops_then_type_then_name():
    doc = {"by_type": {"report": {"items": [node("r", "rep", level=2, confidence_floor="derived", owners=[{"name": "Ann"}, "bob"])]},
                       "table": {"items": [node("b", "b", level=1), node("a", "a", level=1), node("c", "c", level=None)]}}}
    rows = R.impact_rows(doc)
    assert [(r["name"], r["hops"]) for r in rows] == [("a", 1), ("b", 1), ("rep", 2), ("c", None)]
    assert rows[2]["owners"] == ["Ann", "bob"] and rows[2]["confidence"] == "derived"


def test_staleness_rows_and_filter():
    doc = {"nodes": [{"asset": node("a", "a"), "stale": True, "root_cause": True, "last_write_at": "t", "age_hours": 30.0, "last_run": {"state": "failed"}, "reasons": ["old", "failed"]},
                     {"asset": node("b", "b"), "stale": False, "last_run": {"x": 1}}]}
    rows = R.staleness_rows(doc)
    assert [r["name"] for r in rows] == ["a", "b"] and rows[0]["reasons"] == "old; failed" and rows[0]["last_run"] == "failed" and rows[1]["last_run"] == "seen"
    assert [r["name"] for r in R.staleness_rows(doc, only_stale=True)] == ["a"]


def test_step_lines_cover_ok_skip_fail_and_skipped_after_failure():
    result = {"steps": [{"name": "resolve", "ok": True, "detail": "secret value not shown", "duration_ms": 12.5},
                        {"name": "probe", "ok": True, "skipped": True, "detail": "not enabled"},
                        {"name": "token", "ok": False, "detail": "rejected", "duration_ms": 3, "remediation": "fix the secret"},
                        {"name": "list", "ok": False, "skipped": True, "detail": "skipped because an earlier step failed"}]}
    assert R.step_lines(result) == ["1. [ ok ] resolve (12.5 ms) - secret value not shown", "2. [skip] probe - not enabled", "3. [FAIL] token (3 ms) - rejected",
                                    "        fix: fix the secret", "4. [----] list - skipped because an earlier step failed"]
    assert R.step_lines({}) == []


def test_count_by_and_node_index():
    assert R.count_by([{"k": "a"}, {"k": "b"}, {"k": "a"}, {}], "k") == {"a": 2, "b": 1, "None": 1}
    idx = R.node_index({"root": node("r", "root"), "nodes": [node("r", "other"), node("n", "n"), {"name": "no id"}]})
    assert idx["r"]["name"] == "root" and set(idx) == {"r", "n"}
