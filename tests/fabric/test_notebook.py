# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook formats: ipynb, Fabric Git source (python / sql / scala prefixes, MAGIC lines, metadata, parameters, injected parameters) and percent-format scripts; round trips."""

from __future__ import annotations

import json

import pytest

from olcf.core.errors import BadRequest
from olcf.fabric import notebook as nbm
from tests.fabric.fabric_support import ipynb

GIT = """# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11111111-1111-4111-8111-111111111111",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "22222222-2222-4222-8222-222222222222"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Title
# some *text*

# PARAMETERS CELL ********************

n = 5
name = "x"

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from bronze.orders
# MAGIC where id > 1

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

df = spark.range(n)
df.show()
"""


def test_detect_format() -> None:
    assert nbm.detect_format(GIT) == "fabric_git_source"
    assert nbm.detect_format(ipynb([("code", "1")])) == "ipynb"
    assert nbm.detect_format("print(1)") == "py"
    assert nbm.detect_format("x", "fabricGitSource") == "fabric_git_source" and nbm.detect_format("x", "ipynb") == "ipynb"
    assert nbm.detect_format("-- Fabric notebook source\n\nselect 1") == "fabric_git_source"


def test_git_source_parse() -> None:
    nb = nbm.parse(GIT)
    assert [c.cell_type for c in nb.cells] == ["markdown", "code", "code", "code"]
    assert nb.cells[1].is_parameters and nb.cells[1].source == 'n = 5\nname = "x"'
    sql = nb.cells[2]
    assert sql.language == "sql" and sql.magic_prefixed and sql.source.startswith("%%sql\nselect * from bronze.orders")
    assert nb.cells[3].language == "python" and nb.parameters_cell() is nb.cells[1]
    hint = nb.default_lakehouse_hint()
    assert hint and hint["name"] == "bronze" and hint["workspace_id"].startswith("2222")
    assert sql.file_line and GIT.split("\n")[sql.file_line - 1].startswith("# MAGIC %%sql")


def test_git_source_round_trip_is_stable() -> None:
    nb = nbm.parse(GIT)
    again = nbm.parse(nbm.to_git_source(nb))
    assert [(c.cell_type, c.source, c.is_parameters, c.language) for c in again.cells] == [(c.cell_type, c.source, c.is_parameters, c.language) for c in nb.cells]
    assert nbm.to_git_source(again) == nbm.to_git_source(nb)


def test_ipynb_round_trip_and_conversion_between_formats() -> None:
    src = ipynb([("markdown", "# hi"), ("code", "n = 1", ["parameters"]), ("code", "print(n)\nprint(2)")], metadata={"language_info": {"name": "python"}})
    nb = nbm.parse(src)
    assert [c.is_parameters for c in nb.cells] == [False, True, False]
    back = json.loads(nbm.to_ipynb(nb))
    assert [c["source"] if isinstance(c["source"], str) else "".join(c["source"]) for c in back["cells"]] == ["# hi", "n = 1", "print(n)\nprint(2)"]
    assert back["nbformat"] == 4 and back["cells"][1]["metadata"]["tags"] == ["parameters"]
    via_git = nbm.parse(nbm.to_git_source(nb))
    assert [c.source for c in via_git.cells] == [c.source for c in nb.cells] and via_git.cells[1].is_parameters
    assert nbm.serialise(nb, "fabric_git_source").startswith("# Fabric notebook source") and nbm.serialise(nb, "ipynb").lstrip().startswith("{")


def test_ipynb_source_may_be_a_list_of_lines() -> None:
    doc = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {}, "source": ["a = 1\n", "b = 2"], "outputs": [], "execution_count": None}]}
    assert nbm.parse(json.dumps(doc)).cells[0].source == "a = 1\nb = 2"


def test_percent_script() -> None:
    nb = nbm.parse("# %% [markdown]\n# # t\n# %% tags=[\"parameters\"]\nn = 1\n# %%\nprint(n)\n", "py")
    assert [c.cell_type for c in nb.cells] == ["markdown", "code", "code"] and nb.cells[1].is_parameters
    assert nbm.to_py(nb).count("# %%") == 3
    single = nbm.parse("print(1)\nprint(2)\n", "py")
    assert len(single.cells) == 1


def test_sql_prefix_git_source() -> None:
    nb = nbm.parse("-- Fabric notebook source\n\n-- CELL ********************\n\nselect 1;\n")
    assert nb.prefix == "--" and nb.cells[0].language == "sql" and nb.cells[0].source == "select 1;"
    assert nbm.to_git_source(nb).startswith("-- Fabric notebook source")


@pytest.mark.parametrize("bad,fmt", [("not json", "ipynb"), ('{"cells": 5}', "ipynb"), ('{"nbformat": 3, "cells": []}', "ipynb"), ("print(1)", "fabric_git_source"), ("", "ipynb")])
def test_malformed_documents_are_bad_requests(bad: str, fmt: str) -> None:
    with pytest.raises(BadRequest):
        nbm.parse(bad, fmt)


def test_malformed_metadata_block_is_reported() -> None:
    with pytest.raises(BadRequest):
        nbm.parse("# Fabric notebook source\n\n# CELL ********************\n\nx = 1\n\n# METADATA ********************\n\n# META {not json\n")


def test_drop_cells_returns_the_removed_indexes() -> None:
    nb = nbm.parse(ipynb([("code", "a"), ("code", "import olcf_fabric as olcf\nolcf.run_remote()"), ("code", "b")]))
    removed = nbm.drop_cells(nb, lambda c: "run_remote" in c.source)
    assert removed == [1] and [c.source for c in nb.cells] == ["a", "b"] and [c.index for c in nb.cells] == [0, 1]


def test_sha256_text() -> None:
    assert nbm.sha256_text("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_language_aliases() -> None:
    assert [nbm.canonical_language(x) for x in ("pyspark", "synapse_pyspark", "sparksql", "scala", "sparkr", None)] == ["python", "python", "sql", "scala", "r", "python"]
