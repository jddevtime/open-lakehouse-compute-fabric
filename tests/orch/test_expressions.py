# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The pipeline expression language: values, functions, safety limits and error messages."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from olcf.pipelines import expressions as X
from olcf.pipelines.expressions import EvalContext, ExpressionError, evaluate_value

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


@pytest.fixture()
def ctx() -> EvalContext:
    acts = {"A": {"output": {"rows": 5, "items": [1, 2, 3]}, "status": "Succeeded", "error": None}}
    return EvalContext(parameters={"n": 3, "s": "abc", "o": {"a": {"b": [10, 20]}}}, variables={"v": "x"}, pipeline={"RunId": "run1", "Pipeline": "p"},
                       trigger={"body": {"k": 1}, "type": "webhook"}, activity=lambda name: acts.get(name), now=lambda: NOW)


def ev(expr: Any, ctx: EvalContext) -> Any:
    return evaluate_value(expr, ctx)


def err(expr: str, ctx: EvalContext) -> ExpressionError:
    with pytest.raises(ExpressionError) as ei:
        evaluate_value(expr, ctx)
    return ei.value


@pytest.mark.parametrize(("expr", "expected"), [
    ("@add(1,2)", 3),
    ("@add(1.5, 1)", 2.5),
    ("@sub(10, 4)", 6),
    ("@mul(2, 3, 4)", 24),
    ("@div(7, 2)", 3),
    ("@div(-7, 2)", -3),           # integer division truncates toward zero (like .NET)
    ("@div(7.0, 2)", 3.5),
    ("@mod(7, 3)", 1),
    ("@max(1, 5, 3)", 5),
    ("@min(4, 2, 9)", 2),
    ("@round(2.567, 1)", 2.6),
    ("@1 < 2 && 2 < 3", True),
    ("@!true", False),
    ("@equals(1, 1)", True),
    ("@equals(1, '1')", False),    # strict about types
    ("@greater(3, 2)", True),
    ("@lessOrEquals(2, 2)", True),
    ("@not(false)", True),
    ("@and(true, true, false)", False),
    ("@or(false, false, true)", True),
    ("@if(true, 'y', 'n')", "y"),
    ("@coalesce(null, null, 'x')", "x"),
    ("@empty('')", True),
    ("@empty('a')", False),
])
def test_arithmetic_and_logic(expr: str, expected: Any, ctx: EvalContext) -> None:
    got = ev(expr, ctx)
    assert got == expected and type(got) is type(expected)


@pytest.mark.parametrize(("expr", "expected"), [
    ("@concat('a', 1, true)", "a1true"),
    ("@substring('abcdef', 1, 3)", "bcd"),
    ("@replace('aaa', 'a', 'bb')", "bbbbbb"),
    ("@toUpper('abc')", "ABC"),
    ("@toLower('ABC')", "abc"),
    ("@trim('  x ')", "x"),
    ("@split('a,b,c', ',')", ["a", "b", "c"]),
    ("@join(createArray('a', 'b'), '-')", "a-b"),
    ("@startsWith('hello', 'he')", True),
    ("@endsWith('hello', 'lo')", True),
    ("@indexOf('hello', 'l')", 2),
    ("@length('abc')", 3),
    ("@contains('abc', 'b')", True),
    ("@base64('hi')", "aGk="),
    ("@base64ToString('aGk=')", "hi"),
    ("@string(1.5)", "1.5"),
    ("@int('12')", 12),
    ("@float('1.5')", 1.5),
    ("@bool('true')", True),
    ("@json('{\"a\": 1}').a", 1),
])
def test_strings_and_conversions(expr: str, expected: Any, ctx: EvalContext) -> None:
    assert ev(expr, ctx) == expected


@pytest.mark.parametrize(("expr", "expected"), [
    ("@range(2, 3)", [2, 3, 4]),
    ("@take(range(0, 10), 3)", [0, 1, 2]),
    ("@skip(range(0, 5), 3)", [3, 4]),
    ("@first(createArray(7, 8))", 7),
    ("@last(createArray(7, 8))", 8),
    ("@union(createArray(1), createArray(2))", [1, 2]),
    ("@reverse(createArray(1, 2, 3))", [3, 2, 1]),
    ("@length(createArray(1, 2))", 2),
    ("@setProperty(parameters('o'), 'z', 1)", {"a": {"b": [10, 20]}, "z": 1}),
    ("@getProperty(pipeline(), 'RunId')", "run1"),
    ("@keys(object('a', 1, 'b', 2))", ["a", "b"]),
])
def test_collections(expr: str, expected: Any, ctx: EvalContext) -> None:
    assert ev(expr, ctx) == expected


@pytest.mark.parametrize(("expr", "expected"), [
    ("@formatDateTime(utcnow(), 'yyyy-MM-dd')", "2026-01-02"),
    ("@formatDateTime(utcnow(), 'yyyy-MM-ddTHH:mm:ss')", "2026-01-02T03:04:05"),
    ("@addToTime('2026-01-01T00:00:00Z', 1, 'Day')", "2026-01-02T00:00:00.000000Z"),
    ("@startOfDay('2026-03-04T05:06:07Z')", "2026-03-04T00:00:00.000000Z"),
    ("@dayOfWeek('2026-01-01T00:00:00Z')", 4),
    ("@convertFromUtc('2026-07-01T12:00:00Z', 'Europe/Berlin', 'yyyy-MM-dd HH:mm')", "2026-07-01 14:00"),
    ("@convertFromUtc('2026-01-01T12:00:00Z', 'Europe/Berlin', 'yyyy-MM-dd HH:mm')", "2026-01-01 13:00"),
])
def test_dates_use_the_context_clock(expr: str, expected: Any, ctx: EvalContext) -> None:
    assert ev(expr, ctx) == expected


def test_guid_formats(ctx: EvalContext) -> None:
    assert len(ev("@guid()", ctx)) == 36 and len(ev("@guid('N')", ctx)) == 32
    assert ev("@guid('B')", ctx).startswith("{") and ev("@guid('P')", ctx).endswith(")")
    assert err("@guid('X')", ctx).code == "argument_error"
    assert ev("@guid()", ctx) != ev("@guid()", ctx)


def test_run_state_accessors(ctx: EvalContext) -> None:
    assert ev("@pipeline().parameters.n", ctx) == 3
    assert ev("@parameters('s')", ctx) == "abc"
    assert ev("@variables('v')", ctx) == "x"
    assert ev("@pipeline().RunId", ctx) == "run1"
    assert ev("@activity('A').output.rows", ctx) == 5
    assert ev("@activity('A').output.items[1]", ctx) == 2
    assert ev("@activity('A').status", ctx) == "Succeeded"
    assert ev("@activity('A').error?.message", ctx) is None  # null-safe member access
    assert ev("@trigger().body.k", ctx) == 1
    assert ev("@triggerBody().k", ctx) == 1
    assert ev("@parameters('o').a.b[1]", ctx) == 20


def test_item_inside_for_each(ctx: EvalContext) -> None:
    inner = ctx.child(item={"x": 4}, item_index=2, has_item=True)
    assert ev("@item().x", inner) == 4
    assert ev("@itemIndex()", inner) == 2
    assert err("@item()", ctx).code == "no_item"


def test_literals_interpolation_and_escapes(ctx: EvalContext) -> None:
    assert ev("plain", ctx) == "plain"
    assert ev("text @{parameters('n')} and @{variables('v')}", ctx) == "text 3 and x"
    assert ev("@{1}@{2}", ctx) == "12"
    assert ev("@@literal", ctx) == "@literal"
    assert ev("@'it''s'", ctx) == "it's"
    assert ev(5, ctx) == 5
    assert ev(None, ctx) is None
    assert ev({"value": "@add(1,2)", "type": "Expression"}, ctx) == 3  # Fabric style
    assert ev({"a": ["@add(1,1)", "x", {"b": "@concat('p','q')"}]}, ctx) == {"a": [2, "x", {"b": "pq"}]}


def test_short_circuit_and_lazy_branches(ctx: EvalContext) -> None:
    assert ev("@if(true, 1, div(1, 0))", ctx) == 1
    assert ev("@and(false, div(1, 0))", ctx) is False
    assert ev("@or(true, div(1, 0))", ctx) is True
    assert ev("@coalesce('a', div(1, 0))", ctx) == "a"


@pytest.mark.parametrize(("expr", "code"), [
    ("@foo(1)", "unknown_function"),
    ("@add(1)", "arity"),
    ("@add('a', 1)", "type_error"),
    ("@if(1, 2, 3)", "type_error"),
    ("@1/0", "math_error"),
    ("@div(1, 0)", "math_error"),
    ("@1 +", "expression_error"),
    ("@concat(", "expression_error"),
    ("@{1", "expression_error"),
    ("@variables('nope')", "unknown_variable"),
    ("@activity('B').output", "unknown_activity"),
    ("@parameters('s')[10]", "range_error"),
    ("@pipeline().nothing", "missing_property"),
    ("@range(0, 1000000)", "math_error"),
    ("@o", "expression_error"),
])
def test_errors_have_codes_and_messages(expr: str, code: str, ctx: EvalContext) -> None:
    e = err(expr, ctx)
    assert e.code == code
    assert e.message and "Traceback" not in e.message


def test_no_access_to_python_internals(ctx: EvalContext) -> None:
    for expr in ("@parameters('s').__class__", "@pipeline().__dict__", "@pipeline().RunId.upper", "@parameters('s').__len__", "@getProperty(pipeline(), '__class__')"):
        assert isinstance(err(expr, ctx), ExpressionError)
    for expr in ("@__import__('os')", "@eval('1')", "@exec('1')", "@open('/etc/passwd')", "@pipeline().__class__.__mro__"):
        assert isinstance(err(expr, ctx), ExpressionError)


def test_resource_limits(ctx: EvalContext, monkeypatch: pytest.MonkeyPatch) -> None:
    assert "nested too deeply" in err("@" + "(" * 200 + "1" + ")" * 200, ctx).message
    assert "nested too deeply" in err("@" + "concat('a'," * 100 + "'x'" + ")" * 100, ctx).message
    assert "longer than" in err("@" + "1+" * 30000 + "1", ctx).message
    monkeypatch.setattr(X, "MAX_STEPS", 5)
    e = err("@add(add(add(1, 2), add(3, 4)), add(add(5, 6), add(7, 8)))", ctx)
    assert "step" in e.message.lower() or e.code in ("too_complex", "limit")


def test_function_catalogue_is_documented() -> None:
    assert len(X.FUNCTIONS) >= 80
    for name, fn in X.FUNCTIONS.items():
        assert name == name.lower()
        assert fn.doc, f"function {name} has no documentation"
        assert fn.max_args is None or fn.max_args >= fn.min_args


def test_contains_expression_detection() -> None:
    assert X.contains_expression({"a": ["@add(1,2)"]})
    assert X.contains_expression("x @{y}")
    assert not X.contains_expression({"a": ["plain", 1]})
    assert not X.contains_expression("@@escaped")
