# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark job definition analyzer: executed / referenced files, inline PySpark code, compiled-language and file-only gaps, path arguments."""

from __future__ import annotations

from typing import Any

from olcf.intelligence.lineage.spark_job import analyze_spark_job
from olcf.intelligence.model import Facts
from tests.intelligence.lineage.support import (
    asset_labels,
    ctx,
    edges,
    gap_kinds,
    gap_text,
    gaps,
)

CTX = ctx(item_id="itm_job", item_name="job")


def job(defn: Any, **c: Any) -> Facts:
    """Analyse a job definition in the standard test context."""
    return analyze_spark_job(defn, context={**CTX, **c})


def test_file_job_executes_main_file_and_depends_on_reference_files() -> None:
    f = job({"language": "python", "main_file": "Files/jobs/main.py", "reference_files": ["Files/lib/a.py", "Files/lib/b.py"], "args": ["Files/in/x.csv", "--out=Tables/o", "--n=5", "plain"]})
    assert edges(f) == {
        "EXECUTES spark_job:job -> file:Files/jobs/main.py [derived]",
        "DEPENDS_ON spark_job:job -> file:Files/lib/a.py [derived]",
        "DEPENDS_ON spark_job:job -> file:Files/lib/b.py [derived]",
    }
    assert sorted(gaps(f)) == [("not_inspected", "job"), ("unresolved_reference", "job")]
    assert all(e.evidence[0].kind == "definition" for e in f.edges)
    assert {e.attrs["role"] for e in f.edges} == {"main_file", "reference_file"}


def test_job_asset_attrs_and_alias() -> None:
    f = job({"language": "Python", "main_file": "Files/m.py", "args": ["Files/in/x.csv", "--out=Tables/o", "--n=5", "plain"]})
    asset = f.assets[0]
    assert asset_labels(f) == {"spark_job:job"}
    assert dict(asset.ref.alias_map) == {"olcf_item": "itm_job"}
    assert asset.attrs == {"language": "python", "path_arguments": ["Files/in/x.csv", "Tables/o"]}


def test_path_arguments_are_informational_and_create_no_data_edges() -> None:
    f = job({"main_file": "Files/m.py", "args": [1, "abfss://c@a/x", "s3://b/k", "--src=Tables/t", "-x", "Files/in", "Files/in"]})
    assert f.assets[0].attrs["path_arguments"] == ["abfss://c@a/x", "s3://b/k", "Tables/t", "Files/in"]
    assert not edges(f, "READS_FROM", "WRITES_TO")
    arg_gap = next(g for g in f.gaps if g.kind == "unresolved_reference")
    assert arg_gap.location["informational"] is True
    assert "whether the job reads or writes them is not stated" in arg_gap.detail


def test_inline_python_code_is_analysed_with_the_job_as_process() -> None:
    f = job({"language": "python", "code": "df = spark.read.table('src')\ndf.write.saveAsTable('dst')", "args": ["Files/in"]})
    assert edges(f, "READS_FROM", "WRITES_TO") == {"READS_FROM spark_job:job -> table:src [derived]", "WRITES_TO spark_job:job -> table:dst [derived]"}
    # the path argument gap is only for jobs whose code is not visible
    assert [k for k, _ in gaps(f)] == ["unsupported_construct"]
    assert f.assets[0].attrs["path_arguments"] == ["Files/in"]


def test_inline_code_with_main_file_still_executes_the_file() -> None:
    f = job({"code": "spark.read.table('t')", "main_file": "Files/m.py"})
    assert edges(f) == {"EXECUTES spark_job:job -> file:Files/m.py [derived]", "READS_FROM spark_job:job -> table:t [derived]"}
    assert not f.gaps


def test_settings_default_lakehouse_applies_to_inline_code() -> None:
    code = "spark.read.table('t').write.saveAsTable('u')"
    own = job({"code": code})
    other = job({"code": code, "settings": {"default_lakehouse": {"name": "other", "lakehouse_id": "itm_o"}}})
    by_name = job({"code": "spark.read.table('t')", "settings": {"default_lakehouse": {"name": "other"}}})
    scopes = lambda f: {dict(e.dst.scope_map).get("lakehouse_id") or dict(e.dst.scope_map).get("lakehouse") for e in f.edges}  # noqa: E731
    assert scopes(own) == {"itm_lh"} and scopes(other) == {"itm_o"} and scopes(by_name) == {"other"}


def test_scala_and_java_jobs_are_unsupported_not_guessed() -> None:
    for lang in ("scala", "java", "Scala"):
        f = job({"language": lang, "main_class": "com.x.Main", "main_file": "abfss://c@a.dfs.core.windows.net/x.jar", "code": "spark.read.table('t')"})
        assert edges(f) == {"EXECUTES spark_job:job -> file:abfss://c@a.dfs.core.windows.net/x.jar [derived]"}
        assert gap_kinds(f) == {"unsupported_construct"} and "is compiled" in gap_text(f)
        assert f.assets[0].attrs == {"language": lang.lower(), "main_class": "com.x.Main"}


def test_empty_definition_is_not_inspected() -> None:
    for defn in ({"language": "python"}, {"main_file": "  ", "code": "  ", "args": "x"}, {}):
        f = job(defn)
        assert gaps(f) == [("not_inspected", "job")] and not f.edges
        assert "neither main_file nor code" in gap_text(f)


def test_malformed_definitions() -> None:
    bad_defs: list[Any] = [[], "x", None, 5]
    for bad in bad_defs:
        f = job(bad)
        assert gaps(f) == [("parse_error", "job")] and not f.assets and not f.edges
    f = job({"code": "spark.read.table(", "main_file": "x.py"})
    assert gap_kinds(f) == {"parse_error"} and "SyntaxError" in gap_text(f)
    assert edges(f) == {"EXECUTES spark_job:job -> file:x.py [derived]"}


def test_odd_field_types_are_ignored() -> None:
    f = job({"main_file": 5, "reference_files": [1, None, "", " Files/r.py "], "args": {"a": 1}, "settings": "x", "main_class": 3})
    assert edges(f) == {"DEPENDS_ON spark_job:job -> file:Files/r.py [derived]"}
    assert f.assets[0].attrs == {"language": "python"}
    assert gaps(f) == [("not_inspected", "job")]


def test_supplied_process_is_the_job_asset() -> None:
    from dataclasses import replace

    from olcf.intelligence.lineage.base import LineageContext
    from olcf.intelligence.model import AssetRef

    proc = AssetRef.make("spark_job", {"olcf_item": "itm_job"}, "Given")
    f = analyze_spark_job({"code": "spark.read.table('t')"}, context=replace(LineageContext.coerce(CTX), process=proc))
    assert {e.src for e in f.edges} == {proc}
    assert f.assets[0].ref == proc


def test_default_item_name() -> None:
    f = analyze_spark_job({"language": "python"}, context={"item_id": "itm_x"})
    assert gaps(f) == [("not_inspected", "spark job")]
