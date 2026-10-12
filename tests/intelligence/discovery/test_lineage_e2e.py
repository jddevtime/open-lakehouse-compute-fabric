# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End to end with NO stub: the real local connector scans the real sample workspace and the real lineage analyzers turn its notebooks into edges in the real engine.

This is the guard for the shape of the notebook ``DefinitionFact`` payload: OLCF stores a notebook item as ``{"nbformat": <document>, "settings": {...}}`` and the analyzers
expect the nbformat document, the same thing a Fabric notebook delivers.
"""

from __future__ import annotations

from typing import Any

import pytest

from olcf.intelligence.model import DefinitionFact
from olcf.sample import installer
from olcf.security.rbac import SYSTEM_PRINCIPAL

ETL = "01_Retail_Medallion_ETL"


@pytest.fixture()
def sample(disc, monkeypatch):
    """The sample without its own Intelligence part (which scans at install time): these tests drive the scans themselves. ``tests/intelligence/system`` covers the integrated install."""
    from olcf.sample import intelligence as sample_intelligence

    monkeypatch.setattr(sample_intelligence, "integrate", lambda *a, **k: None)
    out = installer.install_sample(disc.ctx, SYSTEM_PRINCIPAL, size="small")
    assert out["action"] == "installed" and "intelligence" not in out
    return out


def edge_names(disc: Any, type_: str, src_id: str | None = None) -> set[tuple[str, str]]:
    names = {a["id"]: a["name"] for a in disc.assets()}
    return {(names[e["src_id"]], names[e["dst_id"]]) for e in disc.edges(type=type_) if src_id is None or e["src_id"] == src_id}


def test_the_sample_etl_notebook_gets_table_and_column_lineage_from_the_real_analyzers(disc, sample):
    row = disc.scan()
    assert row["status"] == "succeeded", (row["error"], [g["detail"][:120] for g in disc.gaps()])
    apply = row["stats"]["apply"]
    assert apply["errors"] == 0 and apply["analyzed"] >= 3                                         # three notebooks, saved queries, a report ... all analysed by the real engine
    assert not [g for g in disc.gaps() if "analyzers are not installed" in g["detail"] or g["kind"] == "parse_error"]
    etl = disc.asset(ETL, "notebook")
    reads = {dst for _, dst in edge_names(disc, "READS_FROM", etl["id"])}
    writes = {dst for _, dst in edge_names(disc, "WRITES_TO", etl["id"])}
    assert {"dim_customer", "dim_product", "dim_store"} <= reads and "Files/raw/sales" in reads
    assert {"silver_sales_enriched", "gold_daily_sales", "gold_category_summary", "gold_customer_ltv"} == writes
    transforms = edge_names(disc, "TRANSFORMS")
    assert ("segment", "customer_segment") in transforms and ("order_date", "year") in transforms and ("order_date", "month") in transforms
    assert all(e["confidence"] in ("derived", "verified", "inferred") for e in disc.edges(type="TRANSFORMS"))
    # the notebook's own asset is the one the local connector discovered (same identity), not a second notebook created by the analyzer
    assert len(disc.assets(type="notebook", name=ETL)) == 1


def test_a_second_scan_does_not_analyse_unchanged_notebooks_again(disc, sample):
    disc.scan()
    n = len(disc.edges())
    again = disc.scan()
    assert again["stats"]["apply"]["analyzed"] == 0 and again["stats"]["apply"]["analysis_skipped"] >= 3
    assert len(disc.edges()) == n


def test_an_edited_notebook_is_analysed_again_and_its_lineage_follows(disc, sample):
    disc.scan()
    items = disc.ctx.service("items")
    nb = items.find(sample["workspace_id"], "notebook", ETL)
    definition = nb["definition"]
    definition["nbformat"]["cells"].append({"cell_type": "code", "metadata": {}, "id": "extra", "execution_count": None, "outputs": [],
                                            "source": "spark.table('fact_web_events').write.saveAsTable('web_copy')"})
    from olcf.core.models import ItemUpdate

    items.update(SYSTEM_PRINCIPAL, nb["id"], ItemUpdate(definition=definition, comment="test edit"), None)
    row = disc.scan()
    assert row["stats"]["apply"]["analyzed"] == 1
    etl = disc.asset(ETL, "notebook")
    assert ("web_copy" in {d for _, d in edge_names(disc, "WRITES_TO", etl["id"])}) and ("fact_web_events" in {d for _, d in edge_names(disc, "READS_FROM", etl["id"])})


def test_the_notebook_payload_is_the_bare_nbformat_document(disc, sample):
    seen: list[DefinitionFact] = []
    real = disc.engine.analyze_and_apply

    def spy(defn, **kw):  # noqa: ANN001, ANN003, ANN202
        if isinstance(defn, DefinitionFact):
            seen.append(defn)
        return real(defn, **kw)

    disc.engine.analyze_and_apply = spy  # type: ignore[method-assign]
    try:
        disc.scan()
    finally:
        disc.engine.analyze_and_apply = real  # type: ignore[method-assign]
    nbs = [d for d in seen if d.kind == "notebook"]
    assert len(nbs) == 3
    for d in nbs:
        assert isinstance(d.payload, dict) and "cells" in d.payload and "settings" not in d.payload and d.payload["nbformat"] == 4
        assert d.context["default_lakehouse"]["name"] == "retail_lakehouse" and d.context["default_lakehouse"].get("id", "").startswith("itm_")
