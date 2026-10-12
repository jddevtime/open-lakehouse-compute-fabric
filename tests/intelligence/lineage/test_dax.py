# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""DAX reference extraction: qualified / unqualified / table references, comments, strings, variables, quoting, and damaged input."""

from __future__ import annotations

import pytest

from olcf.intelligence.lineage.dax import MAX_DAX_CHARS, extract_refs, tokenize


def refs(expression: str) -> list[tuple[str, str | None, str | None]]:
    """``[(kind, table, name)]`` of an expression."""
    return [(r.kind, r.table, r.name) for r in extract_refs(expression).refs]


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("SUM(Sales[Amount])", [("qualified", "Sales", "Amount")]),
        ("SUM('Sales Orders'[Net Amount]) / [Total Qty]", [("qualified", "Sales Orders", "Net Amount"), ("unqualified", None, "Total Qty")]),
        ('CALCULATE([Revenue], FILTER(ALL(Date), Date[Year] = 2025), Customer[Country] = "US")',
         [("unqualified", None, "Revenue"), ("table", "Date", None), ("qualified", "Date", "Year"), ("qualified", "Customer", "Country")]),
        ("COUNTROWS(Sales)", [("table", "Sales", None)]),
        ("COUNTROWS('Fact Sales')", [("table", "Fact Sales", None)]),
        ("SUMX(Sales, Sales[Qty] * RELATED(Product[Price]))", [("table", "Sales", None), ("qualified", "Sales", "Qty"), ("qualified", "Product", "Price")]),
        ("Sales[Net] - [Cost]", [("qualified", "Sales", "Net"), ("unqualified", None, "Cost")]),
        ("[A] + [A] + [a]", [("unqualified", None, "A")]),
        ('EVALUATE SUMMARIZECOLUMNS(Product[Cat], "x", [M])', [("qualified", "Product", "Cat"), ("unqualified", None, "M")]),
        ("SELECTEDVALUE(Date[Y]) IN {1,2}", [("qualified", "Date", "Y")]),
        ("'It''s'[Col]", [("qualified", "It's", "Col")]),
        ("Sales[A]]B]", [("qualified", "Sales", "A]B")]),
        ("1 --comment\n+ Sales[X]", [("qualified", "Sales", "X")]),
        ("", []),
        ("RETURN 1", []),
    ],
)
def test_references(expression: str, expected: list[tuple[str, str | None, str | None]]) -> None:
    assert refs(expression) == expected


def test_comments_and_string_literals_hide_references() -> None:
    assert refs("// Sales[Hidden]\nSUM(Sales[A]) -- Other[B]\n /* Third[C] */") == [("qualified", "Sales", "A")]
    assert refs('"Sales[InString]" & Sales[Real]') == [("qualified", "Sales", "Real")]
    assert refs('"embedded ""Quote"" Sales[X]"') == []


def test_variables_are_not_tables() -> None:
    r = extract_refs("VAR x = SUM(Sales[A]) VAR _t = Date RETURN x * COUNTROWS(_t)")
    assert [(a.kind, a.table, a.name) for a in r.refs] == [("qualified", "Sales", "A"), ("table", "Date", None)]
    assert r.variables == {"x", "_t"}


def test_references_are_case_insensitively_unique_and_ordered_by_first_appearance() -> None:
    assert [r.name for r in extract_refs("[B] + [A] + [b] + [a]").refs] == ["B", "A"]


def test_line_numbers_are_one_based() -> None:
    r = extract_refs("SUM(\n  Sales[A]\n) + \n [B]")
    assert [(x.name, x.line) for x in r.refs] == [("A", 2), ("B", 4)]


@pytest.mark.parametrize(("expression", "problem"), [("SUM(Sales[Amt)", "unterminated ["), ("SUM('Unclosed[Amt])", "unterminated quoted name"), ('"never closed', "unterminated")])
def test_damaged_expressions_report_a_problem_and_never_raise(expression: str, problem: str) -> None:
    r = extract_refs(expression)
    assert any(problem in p for p in r.problems)


def test_overlong_expressions_are_refused_with_a_problem() -> None:
    r = extract_refs("x" * (MAX_DAX_CHARS + 1))
    assert r.refs == [] and "longer than" in r.problems[0]


def test_tokenize_drops_comments_and_keeps_kinds() -> None:
    toks, problems = tokenize("/* c */ Sales[Amount] + 12.5 // tail")
    assert problems == []
    assert [(t.kind, t.value) for t in toks] == [("ident", "Sales"), ("bracket", "Amount"), ("op", "+"), ("num", "12.5")]


@pytest.mark.parametrize("junk", ["\x00\x01\x02", "]]]]", "[[[[", "'''", '"""', "((((", "VAR VAR VAR", "RETURN RETURN", "a[b]c[d]e", "\n\n\n", "é[ü]", "日本[語]"])
def test_junk_never_raises(junk: str) -> None:
    extract_refs(junk)
