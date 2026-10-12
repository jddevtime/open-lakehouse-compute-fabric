# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Power Query (M) analyzer: connectors, column tracking through table functions, native queries, unsupported shapes, and malformed input.

The corpus lives in ``tests/corpus/lineage/mcode``: one ``<case>.m`` file per query and ``expected.json`` with the exact sources / column origins / reasons each query must give
(reviewed by hand when written; ``~`` marks a source whose identity is only approximate). ``shared_expressions.json`` supplies the parameters and shared queries some cases use.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.intelligence.lineage.base import LineageContext
from olcf.intelligence.lineage.mcode import MConn, MResolver, analyze_m, entity_source, parse_m
from tests.intelligence.lineage.support import CORPUS, lab, load_json

MDIR = CORPUS / "mcode"
EXPECTED: dict[str, dict[str, Any]] = load_json("mcode", "expected.json")
SHARED: dict[str, str] = load_json("mcode", "shared_expressions.json")
CTX = LineageContext(default_lakehouse={"name": "lh"})


def summary(text: str, ctx: LineageContext = CTX, shared: dict[str, str] | None = None) -> dict[str, Any]:
    """The canonical description of an analysis (the format of ``expected.json``)."""
    r = analyze_m(text, ctx=ctx, resolver=MResolver(lambda n: (shared if shared is not None else SHARED).get(n)))
    out: dict[str, Any] = {"sources": [f"{s.kind}:{lab(s.ref) if s.ref.type != 'external' else s.ref.name}{'~' if s.approx else ''}" for s in r.sources]}
    if r.table is not None and (r.table.known or r.table.closed):
        out["cols"] = {c: sorted(f"{o.src.label}.{o.col}" for o in (r.origins(c) or [])) for c in r.table.order}
        out["closed"] = r.table.closed
    for k in ("problems", "dynamic", "unsupported"):
        if getattr(r, k):
            out[k] = getattr(r, k)
    if r.reason:
        out["reason"] = r.reason
    return out


@pytest.mark.parametrize("case", sorted(EXPECTED))
def test_corpus_case(case: str) -> None:
    text = (MDIR / f"{case}.m").read_text(encoding="utf-8")
    assert summary(text) == EXPECTED[case]


def test_every_m_file_has_an_expectation() -> None:
    assert {p.stem for p in MDIR.glob("*.m")} == set(EXPECTED)


# ------------------------------------------------------------------------------------------------------------------- spot checks of the corpus
def test_fabric_lakehouse_navigation_gives_a_guid_alias_and_scope() -> None:
    text = (MDIR / "c_lakehouse_nav.m").read_text(encoding="utf-8")
    r = analyze_m(text, ctx=CTX)
    (src,) = r.sources
    assert src.kind == "table"
    assert dict(src.ref.alias_map) == {"fabric": "*/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/sales"}
    assert dict(src.ref.scope) == {"fabric_lakehouse": "22222222-2222-4222-8222-222222222222", "fabric_workspace": "11111111-1111-4111-8111-111111111111"}


def test_column_steps_are_tracked_end_to_end() -> None:
    text = (MDIR / "c_lakehouse_nav.m").read_text(encoding="utf-8")
    steps = text.replace("in\n t", ',\n r = Table.SelectRows(t, each [qty] > 0),\n c = Table.SelectColumns(r, {"order_id","qty"}),\n'
                         ' n = Table.RenameColumns(c, {{"qty","quantity"}}),\n a = Table.AddColumn(n, "double", each [quantity] * 2)\nin\n a')
    r = analyze_m(steps, ctx=CTX)
    assert r.table is not None and r.table.order == ["order_id", "quantity", "double"]
    assert [(o.src.label, o.col) for o in r.origins("quantity") or []] == [("sales", "qty")]
    (o,) = r.origins("double") or []
    assert (o.src.label, o.col, o.text) == ("sales", "qty", "each [quantity] * 2")
    assert r.origins("qty") is None, "a renamed-away column no longer exists"


def test_secrets_in_urls_are_not_kept() -> None:
    blob = json.dumps(EXPECTED)
    assert "SECRET" not in blob and "key=" not in blob


def test_fabric_endpoint_guid_database_is_approximate() -> None:
    r = analyze_m((MDIR / "c_fabric_ep_guid.m").read_text(encoding="utf-8"), ctx=CTX)
    (src,) = r.sources
    assert src.approx and "not mapped to a lakehouse" in src.approx


def test_unknown_server_is_approximate_and_reported() -> None:
    r = analyze_m('let S = Sql.Database(ServerName, "erp"), t = S{[Schema="dbo",Item="X"]}[Data] in t', ctx=CTX)
    (src,) = r.sources
    assert src.ref.name == "sql:*/erp/X" and src.approx == "the sql server is only known at run time"


def test_parameters_and_shared_queries_are_resolved_through_the_resolver() -> None:
    assert EXPECTED["c_param_default"]["sources"] == ["external:sql:srv1/db1/X"]
    assert EXPECTED["c_shared"]["cols"] == {"id": ["dbo.Orders.id"]}


def test_a_connection_that_never_yields_a_table_is_kept_as_an_approximate_source() -> None:
    for case in ("c_web_bare", "c_odata", "c_dynamic_table_name", "c_folder", "c_ssas"):
        assert all(s.endswith("~") for s in EXPECTED[case]["sources"]), case


def test_unknown_functions_keep_the_sources_of_their_arguments() -> None:
    e = EXPECTED["c_unknown_function_over_source"]
    assert e["sources"] == ["external:sql:srv/erp/T"] and "not understood" in e["reason"]


# ---------------------------------------------------------------------------------------------------------------------------- native queries
def test_native_query_columns_come_from_the_sql_analyzer() -> None:
    e = EXPECTED["c_native_cte"]
    assert e["cols"] == {"id": ["dbo.sales.id"], "dbl": ["dbo.sales.amt"]} and e["closed"] is True


def test_native_query_on_a_fabric_endpoint_gives_fabric_tables() -> None:
    assert EXPECTED["c_query_fabric"]["sources"] == ["table:table:mart.sales"]


def test_native_query_select_star_and_dynamic_text_are_not_guessed() -> None:
    assert EXPECTED["c_native_star"]["cols"] == {} and "SELECT *" in EXPECTED["c_native_star"]["reason"]
    assert "not a literal" in EXPECTED["c_native_dyn"]["dynamic"][0]


# ------------------------------------------------------------------------------------------------------------------------ direct lake helper
def test_entity_source_for_a_fabric_sql_endpoint() -> None:
    conn = MConn("sql", server="abc.datawarehouse.fabric.microsoft.com", database="silver")
    src = entity_source(conn, "dbo", "sales", ctx=CTX)
    assert src is not None and src.kind == "table" and src.approx is None
    assert src.ref.name == "sales" and dict(src.ref.scope)["lakehouse"] == "silver"


def test_entity_source_for_onelake_storage_points_at_the_delta_folder() -> None:
    conn = MConn("storage", url="https://onelake.dfs.fabric.microsoft.com/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222")
    src = entity_source(conn, None, "sales", ctx=CTX)
    assert src is not None and src.kind == "table"


def test_entity_source_for_other_connections_is_none() -> None:
    assert entity_source(MConn("web", url="https://example.com"), None, "x", ctx=CTX) is None


# --------------------------------------------------------------------------------------------------------------------------- robustness
@pytest.mark.parametrize("text", ["", "   ", "let", "in", "let a = 1 in", "\x00\x01", "let a = \"unterminated in a", "let a = #\"x in a", "let a = {1,2 in a", "let a = [x=1 in a",
                                  "let a = Sql.Database( in a", "/* never closed", "// only a comment", "let a = 1 in a in a", "(((((", "let a = each each each in a"])
def test_malformed_m_never_raises_and_says_why(text: str) -> None:
    r = analyze_m(text, ctx=CTX)
    assert r.table is None or r.reason or r.table.known is not None
    assert not r.sources
    assert r.problems or r.reason


def test_non_string_input_is_reported() -> None:
    r = analyze_m(None, ctx=CTX)  # type: ignore[arg-type]
    assert r.problems == ["the query is empty"]


def test_parse_m_accepts_comments_and_quoted_identifiers() -> None:
    parse_m('/* c */ let // line\n #"My Step" = 1, b = #"My Step" + 1 in b')


def test_analysis_is_deterministic() -> None:
    texts = [p.read_text(encoding="utf-8") for p in sorted(MDIR.glob("*.m"))]
    assert [summary(t) for t in texts] == [summary(t) for t in texts]


def test_recursive_shared_queries_terminate() -> None:
    shared = {"A": "let x = B in x", "B": "let y = A in y"}
    r = analyze_m("let s = A in s", ctx=CTX, resolver=MResolver(lambda n: shared.get(n)))
    assert not r.sources


def test_many_steps_are_analysed_quickly() -> None:
    steps = ",\n".join(f"s{i} = Table.AddColumn(s{i - 1}, \"c{i}\", each [a] + {i})" for i in range(1, 400))
    text = f'let\n s0 = Sql.Database("srv","erp"){{[Schema="dbo",Item="T"]}}[Data],\n s00 = Table.SelectColumns(s0, {{"a"}}),\n{steps.replace("s0,", "s00,", 1)}\nin\n s399'
    r = analyze_m(text, ctx=CTX)
    assert len(r.sources) == 1


def test_steps_the_result_does_not_use_add_no_sources() -> None:
    text = 'let\n used = Sql.Database("srv","erp"){[Schema="dbo",Item="T"]}[Data],\n unused = Web.Contents("https://example.com/x"),\n unused2 = Sql.Database("other","db")\nin\n used'
    r = analyze_m(text, ctx=CTX)
    assert [s.ref.name for s in r.sources] == ["sql:srv/erp/T"]


def test_a_very_long_step_chain_is_analysed_without_deep_recursion() -> None:
    import time

    steps = ",\n".join(f's{i} = Table.AddColumn(s{i - 1}, "c{i}", each [a] + {i})' for i in range(2, 1500))
    text = f'let\n s0 = Sql.Database("srv","erp"){{[Schema="dbo",Item="T"]}}[Data],\n s1 = Table.SelectColumns(s0, {{"a"}}),\n{steps}\nin\n s1499'
    t0 = time.perf_counter()
    r = analyze_m(text, ctx=CTX)
    assert time.perf_counter() - t0 < 5
    assert not r.problems and len(r.sources) == 1
    assert r.table is not None and len(r.table.order) == 1499


def test_unicode_identifiers_are_valid_m_names() -> None:
    r = analyze_m('let Fuß = Sql.Database("srv", "db"){[Schema="dbo",Item="Größe"]}[Data] in Fuß', ctx=CTX)
    assert not r.problems and len(r.sources) == 1


@pytest.mark.parametrize("text", ["let a\u00b2 = 1 in a\u00b2", "let \u0663 = 1 in 1", "let a = \u0663\u0664 in a", "\u00e9", "\u0663"])
def test_exotic_unicode_never_raises(text: str) -> None:
    r = analyze_m(text, ctx=CTX)
    assert isinstance(r.problems, list) and r.sources == []
