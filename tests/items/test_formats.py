# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook format layer: ipynb / percent .py / Fabric Git source round trips, parameter detection and injection, cell diff, sanitised HTML.  Pure unit tests."""

from __future__ import annotations

import ast
import copy
import json
from html.parser import HTMLParser

import nbformat
import pytest

from olcf.core.errors import BadRequest, Unsupported, ValidationFailed
from olcf.notebooks import diff as nbdiff
from olcf.notebooks import formats as F
from olcf.notebooks import htmlexport as H
from olcf.notebooks import params as P

TRICKY_SOURCES = [
    "print('hi')",
    "%pip install foo==1.0\nimport foo",
    "# %% this line looks like a marker\nx = 1",
    "%%sql\nselect * from t where a = '# %%'",
    "# MAGIC %sql select 1",
    "#~ already escaped\n# METADATA ********************\n# CELL ********************\n# META {}",
    "x = '''\n# %% [markdown]\n'''",
    "\n\nleading blank lines\n\n",
    "trailing spaces   \n\ttabbed",
    "é ü 日本語 🙂",
    "",
]


def make_nb() -> dict:
    return F.normalize_notebook({
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}},
        "cells": [
            {"id": "a1", "cell_type": "markdown", "metadata": {}, "source": "# Title\n\nSome *text*"},
            {"id": "p1", "cell_type": "code", "metadata": {"tags": ["parameters"]}, "execution_count": None, "outputs": [], "source": "alpha = 1\nname = 'x'  # str\nflag = True"},
            *({"id": f"t{i}", "cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": s} for i, s in enumerate(TRICKY_SOURCES)),
        ]})


@pytest.mark.parametrize("fmt", ["ipynb", "py", "fabric"])
def test_round_trip_source_ids_and_tags(fmt):
    nb = make_nb()
    text = F.serialize_notebook(nb, fmt)
    back = F.normalize_notebook(F.parse_notebook(text, fmt))
    assert [c["source"] for c in back["cells"]] == [c["source"] for c in nb["cells"]]
    assert [c["cell_type"] for c in back["cells"]] == [c["cell_type"] for c in nb["cells"]]
    assert [c["id"] for c in back["cells"]] == [c["id"] for c in nb["cells"]]
    assert [F.cell_tags(c) for c in back["cells"]] == [F.cell_tags(c) for c in nb["cells"]]
    # idempotent: serialising the parsed notebook gives the same text (stable diffs)
    assert F.serialize_notebook(back, fmt) == text


@pytest.mark.parametrize("fmt", ["ipynb", "py", "fabric"])
def test_serialisation_is_deterministic(fmt):
    a, b = make_nb(), make_nb()
    assert F.serialize_notebook(a, fmt) == F.serialize_notebook(b, fmt)


def test_sniff_and_autodetect():
    nb = make_nb()
    for fmt in ("ipynb", "py", "fabric"):
        text = F.serialize_notebook(nb, fmt)
        assert F.sniff_format(text) == fmt
        assert F.parse_notebook(text)["cells"]  # no explicit format


def test_ipynb_output_is_valid_nbformat_and_outputs_survive():
    raw = make_nb()
    raw["cells"][2]["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "hi\n"}]
    raw["cells"][2]["execution_count"] = 4
    nb = F.normalize_notebook(raw)
    text = F.to_ipynb_text(nb)
    nbformat.validate(nbformat.reads(text, as_version=4))
    assert F.parse_notebook(text, "ipynb")["cells"][2]["outputs"][0]["text"] == "hi\n"
    stripped = F.parse_notebook(F.to_ipynb_text(nb, strip=True), "ipynb")
    assert stripped["cells"][2]["outputs"] == [] and stripped["cells"][2]["execution_count"] is None


def test_percent_header_metadata_and_cell_attributes():
    text = '# %% [markdown] id="m1"\n# Hello\n\n# %% id="c1" tags=["parameters", "x"]\nn = 3\n'
    nb = F.normalize_notebook(F.parse_notebook(text, "py"))
    assert [c["cell_type"] for c in nb["cells"]] == ["markdown", "code"]
    assert nb["cells"][0]["source"] == "Hello" and nb["cells"][1]["id"] == "c1"
    assert F.cell_tags(nb["cells"][1]) == ["parameters", "x"]
    # a plain Jupytext-style file without ids gets deterministic ids
    nb2 = F.normalize_notebook(F.parse_notebook("# %%\na = 1\n# %%\nb = 2\n", "py"))
    nb3 = F.normalize_notebook(F.parse_notebook("# %%\na = 1\n# %%\nb = 2\n", "py"))
    assert [c["id"] for c in nb2["cells"]] == [c["id"] for c in nb3["cells"]] and len({c["id"] for c in nb2["cells"]}) == 2


def test_fabric_source_markers_and_parameters_cell():
    nb = make_nb()
    text = F.to_fabric_git(nb)
    assert text.startswith("# Fabric notebook source")
    for marker in ("# METADATA ********************", "# CELL ********************", "# MARKDOWN ********************", "# PARAMETERS CELL ********************"):
        assert marker in text
    back = F.normalize_notebook(F.parse_notebook(text, "fabric"))
    assert P.find_parameters_cell(back) is not None
    # a hand written Fabric file (no OLCF ids) parses too
    hand = ("# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   \"kernel_info\": {\n# META     \"name\": \"synapse_pyspark\"\n# META   }\n# META }\n\n"
            "# CELL ********************\n\ndf = spark.range(3)\n\n# METADATA ********************\n\n# META {\n# META   \"language\": \"python\",\n# META   \"language_group\": \"synapse_pyspark\"\n# META }\n")
    parsed = F.normalize_notebook(F.parse_notebook(hand))
    assert parsed["cells"][0]["source"] == "df = spark.range(3)"


def test_fabric_unsupported_language_is_kept_and_warned():
    text = ("# Fabric notebook source\n\n# CELL ********************\n\n# MAGIC %%scala\n# MAGIC val x = 1\n\n# METADATA ********************\n\n# META {\n# META   \"language\": \"scala\"\n# META }\n")
    warnings: list[str] = []
    nb = F.parse_notebook(text, "fabric", warnings=warnings)
    assert any("scala" in w.lower() for w in warnings)
    assert "val x = 1" in F.normalize_notebook(nb)["cells"][0]["source"]


def test_invalid_notebooks_are_rejected():
    with pytest.raises((ValidationFailed, BadRequest, Unsupported)):
        F.parse_notebook("{not json", "ipynb")
    with pytest.raises((ValidationFailed, BadRequest, Unsupported)):
        F.normalize_notebook({"nbformat": 3, "cells": []})
    with pytest.raises((ValidationFailed, BadRequest, Unsupported)):
        F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "source": 5, "metadata": {}, "outputs": [], "execution_count": None}]})
    with pytest.raises((ValidationFailed, BadRequest, Unsupported)):
        F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "weird", "source": "x", "metadata": {}}]})


def test_normalise_is_idempotent_and_assigns_stable_unique_ids():
    raw = {"nbformat": 4, "nbformat_minor": 4, "metadata": {}, "cells": [{"cell_type": "code", "source": ["a = 1\n", "b = 2"], "metadata": {}, "outputs": [], "execution_count": None},
                                                                          {"cell_type": "code", "source": "a = 1\nb = 2", "metadata": {}, "outputs": [], "execution_count": None}]}
    n1 = F.normalize_notebook(copy.deepcopy(raw))
    n2 = F.normalize_notebook(copy.deepcopy(n1))
    assert n1 == n2
    ids = [c["id"] for c in n1["cells"]]
    assert len(set(ids)) == 2 and all(ids)
    assert n1["nbformat_minor"] == 5


# ---------------------------------------------------------------------------------------- parameters
def test_parameter_detection_types_defaults_and_descriptions():
    det = P.detect_parameters(make_nb())
    by = {p.name: p for p in det.parameters}
    assert by["alpha"].type == "integer" and by["alpha"].default == 1
    assert by["name"].type == "string" and by["name"].description == "str"
    assert by["flag"].type == "boolean" and by["flag"].default is True
    nb = F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"id": "p", "cell_type": "code", "metadata": {"tags": ["parameters"]}, "execution_count": None, "outputs": [],
         "source": "ratio: float = 0.5\nitems = [1, 2]\nconf = {'a': 1}\nnothing = None\ncomputed = len('abc')\nneg = -3\n"}]})
    by = {p.name: p for p in P.detect_parameters(nb).parameters}
    assert by["ratio"].type == "number" and by["items"].type == "array" and by["conf"].type == "object"
    assert by["neg"].default == -3 and by["nothing"].default is None
    assert by["computed"].source != "literal"  # not a literal: no default is guessed


def test_no_parameters_cell():
    nb = F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [make_cell := {"id": "x", "cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": "a = 1"}]})
    assert make_cell and P.detect_parameters(nb).parameters == []


def test_coerce_parameters_types_and_unknowns():
    declared = P.detect_parameters(make_nb()).parameters
    assert P.coerce_parameters(declared, {"alpha": "7", "flag": "false"}) == {"alpha": 7, "flag": False}
    assert P.coerce_parameters(declared, {"alpha": 7.0}) == {"alpha": 7}
    with pytest.raises(ValidationFailed):
        P.coerce_parameters(declared, {"alpha": "seven"})
    with pytest.raises(ValidationFailed):
        P.coerce_parameters(declared, {"alpha": True})
    with pytest.raises(ValidationFailed):
        P.coerce_parameters(declared, {"nope": 1}, allow_undeclared=False)


EVIL = ["x'\n__import__('os').system('id')", '"""\nimport os\n"""', "\\", "a b", "\x00", "#", "'; import os; '", "{{ evil }}", "${var:x}", "\r\nprint(1)"]


@pytest.mark.parametrize("evil", EVIL)
def test_injection_is_safe_for_hostile_strings(evil):
    nb = make_nb()
    out = P.inject_parameters(nb, {"name": evil, "alpha": 2})
    cell = out["cells"][2]
    assert F.with_tag(cell, P.INJECTED_TAG)
    tree = ast.parse(cell["source"])  # a valid module that consists only of assignments
    assert all(isinstance(n, ast.Assign) for n in tree.body)
    ns: dict = {}
    exec(compile(tree, "<injected>", "exec"), {"__builtins__": {}}, ns)  # noqa: S102 - the point of the test: executing the cell must only assign
    assert ns["name"] == evil and ns["alpha"] == 2


def test_injection_position_types_and_original_untouched():
    nb = make_nb()
    before = copy.deepcopy(nb)
    out = P.inject_parameters(nb, {"alpha": 5, "flag": False, "items": [1, {"k": None}], "ratio": 0.25})
    assert nb == before  # input is not mutated
    assert F.with_tag(out["cells"][1], P.PARAMETERS_TAG) and F.with_tag(out["cells"][2], P.INJECTED_TAG)  # injected cell directly after the parameters cell
    ns: dict = {}
    exec(out["cells"][2]["source"], {}, ns)  # noqa: S102
    assert ns == {"alpha": 5, "flag": False, "items": [1, {"k": None}], "ratio": 0.25}
    # injecting twice replaces the injected cell (papermill semantics), never stacks
    again = P.inject_parameters(out, {"alpha": 6})
    assert sum(F.with_tag(c, P.INJECTED_TAG) for c in again["cells"]) == 1
    # without a parameters cell the injected cell goes to the top
    bare = F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"id": "x", "cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": "print(a)"}]})
    assert F.with_tag(P.inject_parameters(bare, {"a": 1})["cells"][0], P.INJECTED_TAG)


def test_injection_rejects_non_json_values_and_bad_names():
    nb = make_nb()
    for bad in (object(), {1: 2}, {"a": {1, 2}}, b"bytes"):
        with pytest.raises(ValidationFailed):
            P.inject_parameters(nb, {"alpha": bad})
    for name in ("1bad", "a-b", "a b", "import", "__x__; y", ""):
        with pytest.raises(ValidationFailed):
            P.inject_parameters(nb, {name: 1})
    deep: object = 1
    for _ in range(100):
        deep = [deep]
    with pytest.raises(ValidationFailed):
        P.inject_parameters(nb, {"alpha": deep})


# ---------------------------------------------------------------------------------------- diff
def test_cell_diff_ignores_outputs_and_reports_changes():
    a = make_nb()
    b = copy.deepcopy(a)
    b["cells"][2]["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "noise\n"}]
    b["cells"][2]["execution_count"] = 9
    assert nbdiff.diff_notebooks({"nbformat": a}, {"nbformat": b})["identical"] is True
    b["cells"][1]["source"] = "alpha = 2\nname = 'x'  # str\nflag = True"
    del b["cells"][3]
    b["cells"].append(F.make_cell("code", "brand_new()"))
    d = nbdiff.diff_notebooks({"nbformat": a}, {"nbformat": b})
    st = {c["status"] for c in d["cells"]}
    assert {"modified", "removed", "added"} <= st
    mod = next(c for c in d["cells"] if c["status"] == "modified")
    assert "-alpha = 1" in mod["diff"] and "+alpha = 2" in mod["diff"]
    assert d["summary"]["modified"] == 1 and d["summary"]["removed"] == 1 and d["summary"]["added"] == 1
    assert "alpha = 2" in nbdiff.render_text(d, "v1", "v2")


def test_cell_diff_detects_moves_and_settings():
    a = make_nb()
    b = copy.deepcopy(a)
    b["cells"][3], b["cells"][4] = b["cells"][4], b["cells"][3]
    d = nbdiff.diff_notebooks({"nbformat": a, "settings": {"engine": "duckdb"}}, {"nbformat": b, "settings": {"engine": "spark"}})
    assert d["summary"]["moved"] >= 1 and not d["identical"]
    assert any(s.get("path", "").endswith("engine") or "engine" in json.dumps(s) for s in d["settings"])


# ---------------------------------------------------------------------------------------- html
def test_sanitize_html_removes_active_content():
    dirty = ('<p onclick="x()" style="x">hi<script>alert(1)</script><a href="javascript:alert(1)">l</a><img src=x onerror=y>'
             '<iframe src=z></iframe><svg onload=1><circle/></svg><object data=q></object><a href="https://ok.example/">ok</a><form action=x><input></form></p>')
    clean = H.sanitize_html(dirty).lower()
    for bad in ("<script", "onclick", "onerror", "javascript:", "<iframe", "<svg", "<object", "<form", "<input", "onload", "style="):
        assert bad not in clean
    assert 'href="https://ok.example/"' in clean and "hi" in clean


def test_html_export_is_script_free_and_escapes_everything():
    nb = F.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"id": "m", "cell_type": "markdown", "metadata": {}, "source": "<script>alert(1)</script> [x](javascript:alert(1)) <img src=x onerror=alert(2)> **bold**"},
        {"id": "c", "cell_type": "code", "metadata": {}, "execution_count": 1, "source": "print('<script>alert(3)</script>')",
         "outputs": [{"output_type": "display_data", "data": {"text/html": "<img src=x onerror=alert(4)><b>ok</b><script>alert(5)</script>"}, "metadata": {}},
                         {"output_type": "display_data", "data": {"image/svg+xml": "<svg onload=alert(6)/>"}, "metadata": {}},
                     {"output_type": "stream", "name": "stdout", "text": "<script>alert(7)</script>\x1b[31mred\x1b[0m"},
                     {"output_type": "error", "ename": "E<script>", "evalue": "v", "traceback": ["\x1b[0;31m<script>alert(8)</script>"]}]}]})
    html = H.render_html(nb, "T<script>alert(9)</script>")
    seen_tags: set[str] = set()
    seen_attrs: list[tuple[str, str, str]] = []

    class Collect(HTMLParser):
        def handle_starttag(self, tag, attrs):  # noqa: ANN001
            seen_tags.add(tag)
            seen_attrs.extend((tag, k, v or "") for k, v in attrs)

    Collect().feed(html)
    assert not seen_tags & {"script", "iframe", "object", "embed", "svg", "form", "input", "link"}, seen_tags
    assert not [a for a in seen_attrs if a[1].startswith("on") or a[2].lower().startswith(("javascript:", "data:text/html"))], seen_attrs
    assert "<b>ok</b>" in html and "&lt;script&gt;" in html and "\x1b" not in html
    assert "<title>t&lt;script&gt;" in html.lower()  # the title is escaped too
    assert "default-src 'none'" in H.CSP and "sandbox" in H.CSP
