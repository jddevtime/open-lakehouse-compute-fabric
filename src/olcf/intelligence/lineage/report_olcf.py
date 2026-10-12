# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of an OLCF report (dashboard): which tables and columns each tile shows.

An OLCF ``report`` item is ``{"title", "filters": [...], "tiles": [{"id", "title", "type", "sql", "engine", "lakehouse_id", ...}]}``. :func:`analyze_report_olcf` ``(defn, *, context)``
analyses every tile's SQL with the SQL analyzer (``duckdb`` or ``spark`` as the tile's ``engine`` says) and emits

* the report asset (the context's process asset) and one ``visual`` asset per tile (parent = report; ``source`` alias ``<source id or olcf>:<report item id>/tile/<tile id>`` when the
  report's item id is known);
* ``READS_FROM`` tile -> table / view / file for every dataset the tile's query reads, and the same ``READS_FROM`` report -> dataset (the report depends on everything its tiles read);
* ``DISPLAYS`` tile -> source column for every output column of the tile query that can be traced to a table column (``attrs.as`` lists the output names it feeds; a column computed from several
  source columns gives one edge per source column), (``derived``);
* ``READS_FROM`` report -> table for the ``options_sql`` of a ``select`` filter.

``:name`` bind parameters of the tiles are not data sources and are ignored. A tile without SQL, or whose SQL cannot be parsed, gives a gap (``not_inspected`` / ``parse_error``) and the
other tiles are still analysed. Output columns that cannot be traced (a literal, a ``SELECT *`` over a table with unknown columns, an ambiguous unqualified name) get no ``DISPLAYS`` edge and the
SQL analyzer's gap (``unresolved_reference`` / ``unsupported_construct``) says why; nothing is guessed.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from olcf.intelligence.lineage.base import (
    REPORT_OLCF,
    FactBuilder,
    LineageContext,
    column_ref,
    ctx_for_lakehouse,
    facts_with_gap,
)
from olcf.intelligence.lineage.sql import query_outputs
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip

MAX_TILES = 500


def _lakehouse(ctx: LineageContext, value: Any) -> LineageContext:
    """The context a tile runs in: its ``lakehouse_id`` when that is an OLCF item id (``itm_...``), otherwise the report's own default lakehouse."""
    if isinstance(value, str) and value.startswith("itm_") and value != (ctx.default_lakehouse or {}).get("id"):
        return ctx_for_lakehouse(ctx, value)
    return ctx


def analyze_report_olcf(defn: Any, *, context: Any = None) -> Facts:
    """Analyse an OLCF report definition (see the module docstring). ``defn`` is the report's definition dict; never raises."""
    ctx = LineageContext.coerce(context)
    title = ctx.item_name or (defn.get("title") if isinstance(defn, dict) and isinstance(defn.get("title"), str) else None) or "report"
    if not isinstance(defn, dict):
        return facts_with_gap(REPORT_OLCF, "parse_error", title, "the report definition is not an object", "Check the report item's definition.", ctx.loc())
    b = FactBuilder(REPORT_OLCF, ctx)
    report_ref = ctx.process or AssetRef.make("dashboard", {"olcf_item": ctx.item_id} if ctx.item_id else None, title, ctx.scope())
    tiles = defn.get("tiles")
    filters = defn.get("filters")
    b.asset(AssetFact(ref=report_ref, name=title, attrs={"tile_count": len(tiles) if isinstance(tiles, list) else 0, "filter_count": len(filters) if isinstance(filters, list) else 0},
                      workspace_id=ctx.workspace_id))
    if tiles is not None and not isinstance(tiles, list):
        b.gap("parse_error", title, "'tiles' is not a list", None, ctx.loc())
        return b.facts()
    seen: set[str] = set()
    for i, tile in enumerate(tiles or []):
        if i >= MAX_TILES:
            b.gap("unsupported_construct", title, f"the report has more than {MAX_TILES} tiles; the rest were not analysed", None, ctx.loc())
            break
        _tile(b, ctx, report_ref, title, i, tile, seen)
    for j, flt in enumerate(filters if isinstance(filters, list) else []):
        if isinstance(flt, dict) and isinstance(flt.get("options_sql"), str) and flt["options_sql"].strip():
            fctx = replace(_lakehouse(ctx, flt.get("options_lakehouse_id")), location={**ctx.location, "filter": str(flt.get("name") or j)})
            out = query_outputs(flt["options_sql"], dialect="duckdb", context=fctx, process=report_ref)
            b.merge(out.facts)
    return b.facts()


def _tile(b: FactBuilder, ctx: LineageContext, report_ref: AssetRef, title: str, index: int, tile: Any, seen: set[str]) -> None:
    """Analyse one tile: its asset, the datasets it reads and the columns it displays. ``seen`` holds the tile ids already used (a repeated id is renamed ``<id>#<n>`` with a gap)."""
    if not isinstance(tile, dict):
        b.gap("parse_error", f"{title}/tile {index}", "a tile is not an object", None, ctx.loc(tile=index))
        return
    tid = str(tile.get("id") or f"tile{index}")
    if tid in seen:
        unique, n = f"{tid}#2", 2
        while unique in seen:
            n += 1
            unique = f"{tid}#{n}"
        b.gap("parse_error", f"{title}/{tid}", f"two tiles share the id '{tid}'; the tile at position {index} is analysed as '{unique}'", "Give every tile a unique id.", ctx.loc(tile=index))
        tid = unique
    seen.add(tid)
    ttitle = str(tile.get("title") or tid)
    sql = tile.get("sql")
    alias = {"source": f"{ctx.source_id or 'olcf'}:{ctx.item_id}/tile/{tid}"} if ctx.item_id else None
    ref = AssetRef.make("visual", alias, ttitle, {"report": ctx.item_id or title, "tile": tid})
    b.asset(AssetFact(ref=ref, name=ttitle, attrs={"tile_id": tid, "tile_type": tile.get("type"), "engine": tile.get("engine")}, parent=report_ref, workspace_id=ctx.workspace_id,
                      path=f"{title} / {ttitle}"))
    tctx = replace(_lakehouse(ctx, tile.get("lakehouse_id")), location={**ctx.location, "tile": tid})
    if not isinstance(sql, str) or not sql.strip():
        b.gap("not_inspected", f"{title}/{ttitle}", "the tile has no SQL", None, tctx.loc())
        return
    dialect = "spark" if str(tile.get("engine") or "duckdb").lower() == "spark" else "duckdb"
    out = query_outputs(sql, dialect=dialect, context=tctx, process=ref)
    b.merge(out.facts)
    for e in out.facts.edges:
        if e.type == "READS_FROM":
            b.edge(report_ref, e.dst, "READS_FROM", confidence=e.effective_confidence(), kind="static", location=tctx.loc(), snippet=clip(sql, 400))
    shown: dict[tuple[AssetRef, str], list[str]] = {}
    for name, origins in out.columns:
        for table, col in origins:
            names = shown.setdefault((table, col), [])
            if name not in names:
                names.append(name)
    for (table, col), names in shown.items():
        b.edge(ref, column_ref(table, col), "DISPLAYS", confidence="derived", kind="static", location=tctx.loc(), snippet=clip(sql, 400), attrs={"as": ", ".join(names)})


__all__ = ["analyze_report_olcf"]
