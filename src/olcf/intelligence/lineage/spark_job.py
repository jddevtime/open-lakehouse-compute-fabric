# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of an OLCF Spark job definition (``spark_job_definition`` item).

A job definition names *what runs*, not what the code does: ``{"language": "python|scala|java", "main_file" | "code", "main_class", "args", "reference_files", "settings"}``.
:func:`analyze_spark_job` ``(defn, *, context)`` therefore emits only what the definition states:

* the job asset (the context's process asset) with its language, main class and the arguments that look like storage paths (``attrs.path_arguments``);
* ``EXECUTES`` job -> main file (a ``file`` reference to the ``main_file``) and ``DEPENDS_ON`` job -> each reference file (code that flows into the job);
* for **inline Python** (``code``): the full PySpark analysis of that code (see :mod:`olcf.intelligence.lineage.pyspark`), run with the job as the process and with
  ``settings.default_lakehouse`` as the default lakehouse;
* a ``not_inspected`` gap when the code is only a file reference (the analyzers never read storage, so the tables the job reads and writes are unknown until a run is observed), an
  ``unsupported_construct`` gap for Scala / Java code, and an ``unresolved_reference`` gap listing arguments that look like paths: whether such an argument is read or written is
  unknowable, so no ``READS_FROM`` / ``WRITES_TO`` edge is guessed for it.

Facts about the job's data come from runtime evidence (the compute controller's ``dataset.read`` / ``dataset.written`` events) when it is enabled.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from olcf.intelligence.lineage.base import (
    SPARK_JOB,
    FactBuilder,
    LineageContext,
    classify_path,
    ctx_for_lakehouse,
)
from olcf.intelligence.lineage.pyspark import analyze_python
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip

_PATHISH = re.compile(r"^(?:(?:Files|Tables)/|/lakehouse/|(?:abfss?|wasbs?|s3a?|gs|olcf|hdfs|https?)://)", re.IGNORECASE)


def _path_args(args: Any) -> list[str]:
    """Arguments (or the value part of ``--flag=value``) that look like lakehouse paths or storage URIs, in order, de-duplicated."""
    out: list[str] = []
    for a in args if isinstance(args, list) else []:
        if not isinstance(a, str):
            continue
        value = a.split("=", 1)[1] if a.startswith("-") and "=" in a else a
        if _PATHISH.match(value) and value not in out:
            out.append(value)
    return out


def _job_context(ctx: LineageContext, settings: Any) -> LineageContext:
    """The context the job's code runs in: ``settings.default_lakehouse`` (``{"lakehouse_id" | "name"}``) when given."""
    lh = settings.get("default_lakehouse") if isinstance(settings, dict) else None
    if isinstance(lh, dict):
        target = lh.get("lakehouse_id") or lh.get("id") or lh.get("name")
        if isinstance(target, str) and target:
            return ctx_for_lakehouse(ctx, target)
    return ctx


def analyze_spark_job(defn: Any, *, context: Any = None) -> Facts:
    """Analyse a ``spark_job_definition`` dict (see the module docstring); never raises."""
    ctx = LineageContext.coerce(context)
    b = FactBuilder(SPARK_JOB, ctx)
    name = ctx.item_name or "spark job"
    if not isinstance(defn, dict):
        b.gap("parse_error", name, "the Spark job definition is not an object", "Check the item's definition.", ctx.loc())
        return b.facts()
    try:
        _analyze(defn, ctx, b, name)
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        b.gap("parse_error", name, f"Spark job analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the definition to the OLCF maintainers.", ctx.loc())
    return b.facts()


def _analyze(defn: dict[str, Any], ctx: LineageContext, b: FactBuilder, name: str) -> None:
    job = ctx.process or AssetRef.make("spark_job", {"olcf_item": ctx.item_id} if ctx.item_id else None, name, ctx.scope())
    language = str(defn.get("language") or "python").lower()
    path_args = _path_args(defn.get("args"))
    attrs: dict[str, Any] = {"language": language}
    if isinstance(defn.get("main_class"), str):
        attrs["main_class"] = defn["main_class"]
    if path_args:
        attrs["path_arguments"] = path_args[:50]
    b.asset(AssetFact(ref=job, name=name, attrs=attrs, workspace_id=ctx.workspace_id))
    jctx = replace(_job_context(ctx, defn.get("settings")), process=job)
    main = defn.get("main_file")
    code = defn.get("code")
    loc = ctx.loc()
    if isinstance(main, str) and main.strip():
        target = classify_path(main.strip())
        b.edge(job, target.ref(jctx), "EXECUTES", confidence="derived", kind="definition", location=loc, snippet=clip(main, 200), attrs={"role": "main_file"})
    refs = defn.get("reference_files")
    for ref_file in refs if isinstance(refs, list) else []:
        if isinstance(ref_file, str) and ref_file.strip():
            b.edge(job, classify_path(ref_file.strip()).ref(jctx), "DEPENDS_ON", confidence="derived", kind="definition", location=loc, snippet=clip(ref_file, 200),
                   attrs={"role": "reference_file"})
    if language in ("scala", "java"):
        b.gap("unsupported_construct", name, f"{language} job code is compiled; only Python code is analysed, so the datasets the job reads and writes are unknown",
              "Runtime lineage (dataset events) covers compiled jobs.", loc)
    elif isinstance(code, str) and code.strip():
        b.merge(analyze_python(code, context=jctx))
    elif isinstance(main, str) and main.strip():
        b.gap("not_inspected", name, f"the job runs '{clip(main, 120)}'; the code of the main file is not part of the definition, so the datasets it reads and writes are unknown",
              "Run the job with lineage events enabled, or put the code inline in the definition's 'code'.", loc)
    else:
        b.gap("not_inspected", name, "the job definition has neither main_file nor code", None, loc)
    if path_args and not (isinstance(code, str) and code.strip()):
        shown = ", ".join(path_args[:5]) + (f" and {len(path_args) - 5} more" if len(path_args) > 5 else "")
        b.gap("unresolved_reference", name, f"arguments that look like data locations ({shown}) are passed to the job; whether the job reads or writes them is not stated",
              "Informational: no READS_FROM / WRITES_TO edge is created for them.", {**loc, "informational": True})


__all__ = ["analyze_spark_job"]
