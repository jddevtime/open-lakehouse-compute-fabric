# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Entry point of the lineage analyzers: :func:`analyze_definition` turns one stored definition into :class:`~olcf.intelligence.model.Facts`.

The dispatch is by ``DefinitionFact.kind`` (``olcf.intelligence.model.DEFINITION_KINDS``):

=================  ==============================================================================
``notebook``        :func:`~olcf.intelligence.lineage.notebook.analyze_notebook`
``pipeline_olcf``   :func:`~olcf.intelligence.lineage.pipeline_olcf.analyze_olcf_pipeline`
``pipeline_fabric`` :func:`~olcf.intelligence.lineage.pipeline_fabric.analyze_fabric_pipeline`
``sql_query``       :func:`~olcf.intelligence.lineage.sql.analyze_sql` (payload ``{"sql", "engine", "lakehouse_id"}``)
``report_olcf``     :func:`~olcf.intelligence.lineage.report_olcf.analyze_report_olcf`
``spark_job_olcf``  :func:`~olcf.intelligence.lineage.spark_job.analyze_spark_job`
``semantic_model``  :func:`~olcf.intelligence.lineage.semantic_model.analyze_semantic_model`
``report_pbi``      :func:`~olcf.intelligence.lineage.report_pbi.analyze_report_pbi`
``scanner_result``  :func:`~olcf.intelligence.lineage.scanner.analyze_scanner_result`
=================  ==============================================================================

Guarantees: the analysis is pure (no database, no network, no code execution, no file reads), deterministic (same definition and context give an identical result) and never raises. An
unknown kind returns one ``not_inspected`` gap; an analyzer that fails unexpectedly is reported as a ``parse_error`` gap that names the analyzer. The process asset of the definition is
always part of the result (analyzers that did not emit it get a minimal one), and every evidence record without a ``ref`` is stamped with the definition's etag, so a later scan can tell
which version of the definition produced an edge.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from olcf.intelligence.lineage.base import (
    ANALYZER_VERSION,
    ENGINE,
    FactBuilder,
    LineageContext,
    ctx_for_lakehouse,
    ref_key,
)
from olcf.intelligence.lineage.notebook import analyze_notebook
from olcf.intelligence.lineage.pipeline_fabric import analyze_fabric_pipeline
from olcf.intelligence.lineage.pipeline_olcf import analyze_olcf_pipeline
from olcf.intelligence.lineage.report_olcf import analyze_report_olcf
from olcf.intelligence.lineage.report_pbi import analyze_report_pbi
from olcf.intelligence.lineage.scanner import analyze_scanner_result
from olcf.intelligence.lineage.semantic_model import analyze_semantic_model
from olcf.intelligence.lineage.spark_job import analyze_spark_job
from olcf.intelligence.lineage.sql import analyze_sql
from olcf.intelligence.model import DEFINITION_KINDS, AssetFact, DefinitionFact, Facts

Analyzer = Callable[[Any, LineageContext], Facts]


def _sql_query(payload: Any, ctx: LineageContext) -> Facts:
    """``sql_query`` payload ``{"sql", "engine", "lakehouse_id"}`` (a bare string is taken as SQL)."""
    if isinstance(payload, str):
        payload = {"sql": payload}
    if not isinstance(payload, dict):
        return Facts(gaps=[_gap("sql-analyzer", "parse_error", ctx, "the sql_query payload is not an object with a 'sql' text")])
    sql = payload.get("sql")
    if not isinstance(sql, str):
        return Facts(gaps=[_gap("sql-analyzer", "parse_error", ctx, "the sql_query payload has no 'sql' text")])
    engine = str(payload.get("engine") or "").lower()
    dialect = engine if engine in ("duckdb", "spark") else ctx.dialect
    lakehouse = payload.get("lakehouse_id")
    run_ctx = ctx_for_lakehouse(ctx, lakehouse) if isinstance(lakehouse, str) and lakehouse.startswith("itm_") and lakehouse != (ctx.default_lakehouse or {}).get("id") else ctx
    return analyze_sql(sql, dialect=dialect, context=run_ctx)


def _gap(analyzer: str, kind: str, ctx: LineageContext, detail: str) -> Any:
    b = FactBuilder(analyzer, ctx)
    b.gap(kind, ctx.item_name or "definition", detail, None, ctx.loc())
    return b.facts().gaps[0]


#: ``kind -> (analyzer name used in gaps, function(payload, ctx))``
ANALYZERS: dict[str, tuple[str, Analyzer]] = {
    "notebook": ("notebook-analyzer", lambda p, c: analyze_notebook(p, context=c)),
    "pipeline_olcf": ("olcf-pipeline-analyzer", lambda p, c: analyze_olcf_pipeline(p, context=c)),
    "pipeline_fabric": ("fabric-pipeline-analyzer", lambda p, c: analyze_fabric_pipeline(p, context=c)),
    "sql_query": ("sql-analyzer", _sql_query),
    "report_olcf": ("olcf-report-analyzer", lambda p, c: analyze_report_olcf(p, context=c)),
    "spark_job_olcf": ("spark-job-analyzer", lambda p, c: analyze_spark_job(p, context=c)),
    "semantic_model": ("semantic-model-analyzer", lambda p, c: analyze_semantic_model(p, context=c)),
    "report_pbi": ("pbi-report-analyzer", lambda p, c: analyze_report_pbi(p, context=c)),
    "scanner_result": ("pbi-scanner-analyzer", lambda p, c: analyze_scanner_result(p, context=c)),
}


def analyze_definition(defn: DefinitionFact) -> Facts:
    """Analyse one stored definition and return its lineage facts; never raises (see the module docstring)."""
    kind = getattr(defn, "kind", None)
    ctx = LineageContext.from_definition(defn)
    entry = ANALYZERS.get(kind) if isinstance(kind, str) else None
    if entry is None:
        known = ", ".join(DEFINITION_KINDS)
        return Facts(gaps=[_gap(ENGINE, "not_inspected", ctx, f"definition kind '{kind}' has no lineage analyzer (known kinds: {known})")])
    analyzer, fn = entry
    try:
        facts = fn(defn.payload, ctx)
    except Exception as exc:  # noqa: BLE001 - the contract is that analysis never raises
        return Facts(gaps=[_gap(analyzer, "parse_error", ctx, f"{analyzer} failed unexpectedly on this {kind} definition ({type(exc).__name__}: {str(exc)[:200]})")])
    return _finish(defn, facts, ctx, str(kind))


def _finish(defn: DefinitionFact, facts: Facts, ctx: LineageContext, kind: str) -> Facts:
    """Make sure the process asset is present and stamp the definition etag on the evidence."""
    process = defn.ref
    wanted = set(process.aliases)
    if not any(ref_key(a.ref) == ref_key(process) or (wanted and wanted & set(a.ref.aliases)) for a in facts.assets):
        facts.assets.insert(0, AssetFact(ref=process, name=defn.name, attrs={"definition_kind": kind, "analyzer_version": ANALYZER_VERSION}, workspace_id=ctx.workspace_id))
    if defn.etag:
        for e in facts.edges:
            for ev in e.evidence:
                if ev.ref is None:
                    ev.ref = defn.etag
    return facts


__all__ = ["ANALYZERS", "ANALYZER_VERSION", "analyze_definition"]
