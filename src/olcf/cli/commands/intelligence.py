# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf intelligence`` - search the asset graph, trace lineage down to columns, find where data comes from and what a change breaks, and run discovery.

Everything here is a thin layer over :class:`olcf.sdk.Client` ``.intelligence`` (``/v1/intelligence/*``); the terminal renderers live in :mod:`olcf.cli.intel_render`.

* Assets are addressed by id (``ast_...``), by name (``clean``), by path (``lakehouse_a/clean``) or by dotted name (``lakehouse_a.clean``, ``clean.amount`` for a column).
  A name that matches several assets is refused with the candidates listed - pass the id or ``--type``.
* Every command accepts ``--json`` (and the global ``-o json|yaml|csv``); messages go to stderr, data to stdout; exit codes are the documented ones (``olcf --help``).
* Secrets of a Fabric source are read from an environment variable, a file or a hidden prompt - never from an argument, never echoed, never logged.
"""

from __future__ import annotations

import functools
import json
import re
from collections.abc import Callable
from typing import Any

import click

from olcf.cli import intel_render as R
from olcf.cli.common import confirm, parse_kv, pass_state, read_input, secret_prompt, yes_option
from olcf.cli.output import human_age
from olcf.cli.state import EXIT_FAIL, EXIT_TIMEOUT, State
from olcf.sdk import BadRequestError, NotFoundError, OlcfTimeoutError

CONFIDENCES = ["verified", "derived", "inferred", "unknown"]
SOURCE_KINDS = ["local", "catalog_mirror", "fabric", "scanner_import"]
ASSET_STATUSES = ["active", "removed", "placeholder", "any"]
SCAN_STATUSES = ["running", "succeeded", "partial", "failed", "cancelled"]
HELP_FOOTER = "\n\nAll commands take --json (or the global -o json|yaml|csv) and use the documented exit codes (see `olcf --help`)."


# --------------------------------------------------------------------------------------------- shared plumbing
def json_option(f: Callable[..., Any]) -> Callable[..., Any]:
    """``--json`` on the command itself (equivalent to the global ``--json``/``-o json``), so ``olcf intelligence lineage X --json`` works."""

    @click.option("--json", "json_flag", is_flag=True, help="Print JSON (same as the global --json).")
    @functools.wraps(f)
    def wrapper(*a: Any, json_flag: bool = False, **kw: Any) -> Any:
        if json_flag:
            st = click.get_current_context().find_object(State)
            if st is not None:
                st.fmt = st.out.fmt = "json"
        return f(*a, **kw)

    return wrapper


def conf_style(state: State) -> R.Style:
    """Colour for confidence words (verified green, derived cyan, inferred yellow, unknown red) and ok/warn/fail tags; plain without a terminal."""
    colours = {"verified": "green", "derived": "cyan", "inferred": "yellow", "unknown": "red", "ok": "green", "warn": "yellow", "fail": "red"}

    def style(text: str, role: str = "") -> str:
        return state.out.style(text, fg=colours.get(role)) if role in colours else text

    return style


def _not_found(message: str, hint: str) -> NotFoundError:
    return NotFoundError(message, status=404, code="not_found", remediation=hint)


def _candidate_key(a: dict[str, Any]) -> str:
    return f"{a.get('id')}  {a.get('type')}  {a.get('path') or a.get('name')}  ({a.get('workspace_id') or 'organisation'})"


def _exact(a: dict[str, Any], ref: str) -> bool:
    """Does ``ref`` name the asset exactly (its name, its path or the tail of its path; ``.`` works like ``/`` and the spaces around ``/`` in display paths such as ``Sales / lh / t`` are ignored)?"""
    r = re.sub(r"\s*/\s*", "/", ref.strip().lower())
    forms = {r, r.replace(".", "/")}
    name, path = str(a.get("name") or "").lower(), re.sub(r"\s*/\s*", "/", str(a.get("path") or "").lower())
    return name in forms or path in forms or any(path.endswith("/" + f) for f in forms)


def resolve_asset(state: State, ref: str, type_: str | None = None) -> str:
    """The asset id for an id, a name, a path or a dotted name (``clean.amount`` finds the column ``amount`` of the table ``clean``).

    Raises :class:`~olcf.sdk.NotFoundError` (exit 5) when nothing matches and :class:`~olcf.sdk.BadRequestError` (exit 7, candidates listed) when several assets match.
    """
    ref = ref.strip()
    if ref.startswith("ast_"):
        return ref
    c = state.client.intelligence
    hits: dict[str, dict[str, Any]] = {}
    last = ref.replace("/", ".").rsplit(".", 1)[-1]
    for q in dict.fromkeys([ref, ref.replace(".", "/"), last]):
        for a in c.list_assets(q=q, type=type_, limit=100):
            if _exact(a, ref):
                hits[a["id"]] = a
        if hits:
            break
    if not hits and "." in ref and not type_:
        parent_ref, col = ref.rsplit(".", 1)
        try:
            parent = resolve_asset(state, parent_ref)
        except (NotFoundError, BadRequestError):
            parent = None
        if parent:
            for ch in c.children(parent, type="column", limit=500):
                if str(ch.get("name") or "").lower() == col.lower():
                    hits[ch["id"]] = ch
    if not hits:
        raise _not_found(f"no asset matches '{ref}'", "search with `olcf intelligence search TEXT`, or pass the asset id (ast_...)")
    if len(hits) > 1:
        live = {k: v for k, v in hits.items() if v.get("status") == "active"}
        hits = live if len(live) == 1 else hits
    if len(hits) > 1:
        shown = "\n  ".join(_candidate_key(a) for a in list(hits.values())[:12])
        raise BadRequestError(f"'{ref}' matches {len(hits)} assets:\n  {shown}", status=400, code="ambiguous_asset",
                              remediation="pass the asset id (ast_...), a longer path such as lakehouse/table, or --type")
    return next(iter(hits))


def source_names(state: State) -> dict[str, str]:
    """``{source id: source name}`` for readable tables (an unknown id is shown as it is)."""
    try:
        return {str(x["id"]): str(x["name"]) for x in state.client.intelligence.list_sources()}
    except (NotFoundError, BadRequestError):
        return {}


def asset_argument(f: Callable[..., Any]) -> Callable[..., Any]:
    """``ASSET`` argument plus ``--type`` (to disambiguate a name shared by assets of different types)."""

    @click.argument("asset")
    @click.option("--type", "asset_type", default=None, help="Only consider assets of this type when resolving ASSET (table, column, notebook, report ...).")
    @functools.wraps(f)
    def wrapper(*a: Any, **kw: Any) -> Any:
        return f(*a, **kw)

    return wrapper


def all_pages(fn: Callable[..., Any], *, limit: int, fetch_all: bool, **kw: Any) -> list[dict[str, Any]]:
    """One page, or - with ``fetch_all`` - every page of a list method."""
    if not fetch_all:
        return list(fn(limit=limit, **kw))
    out: list[dict[str, Any]] = []
    cursor = None
    while True:
        page = fn(limit=limit, cursor=cursor, **kw)
        out.extend(page)
        cursor = getattr(page, "next_cursor", None)
        if not cursor:
            return out


def paging_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--all", "fetch_all", is_flag=True, help="Follow every page (--limit is then the page size).")(f)
    return click.option("--limit", type=click.IntRange(1, 500), default=50, show_default=True, help="Rows per page.")(f)


ASSET_COLS = [("ID", "id", 30), ("TYPE", "type"), ("NAME", "name", 30), ("PATH", "path", 40), ("WORKSPACE", "workspace_id", 28), ("STATUS", "status")]


# --------------------------------------------------------------------------------------------- group
@click.group("intelligence", help="Asset graph, lineage, discovery sources and sampling policies (OLCF Intelligence).",
             epilog="\b\nExamples:\n"
                    "  olcf intelligence search sales                        find assets by name, path, description or alias\n"
                    "  olcf intelligence lineage lakehouse_a.clean            what feeds a table and what it feeds (tree)\n"
                    "  olcf intelligence lineage clean.amount --level column  column-level lineage as a table\n"
                    "  olcf intelligence where-from sales_report              where does it originate? (roots, paths, confidence)\n"
                    "  olcf intelligence impact raw                           what breaks if raw changes?\n"
                    "  olcf intelligence sources add crm --kind fabric --tenant-id T --client-id C\n"
                    "  olcf intelligence sources test crm                     step-by-step connectivity test with remediation\n"
                    "  olcf intelligence coverage                             what has been discovered and what could not be read" + HELP_FOOTER)
def intelligence_group() -> None:
    pass


# --------------------------------------------------------------------------------------------- summary
@intelligence_group.command("summary", help="Counts of assets, edges by confidence, open gaps and the latest scan per source.",
                            epilog="\b\nExample:\n  olcf intelligence summary --json")
@json_option
@pass_state
def summary_cmd(state: State) -> None:
    def human(d: dict[str, Any]) -> None:
        o = state.out
        a, e, g = d.get("assets") or {}, d.get("edges") or {}, d.get("gaps") or {}
        o.kv([("assets", f"{a.get('total', 0)}  ({_counts(a.get('by_type'))})"), ("edges", f"{e.get('total', 0)}  ({_counts(e.get('by_confidence'))})"),
              ("placeholders", d.get("placeholders", 0)), ("open gaps", f"{g.get('open', 0)}  ({_counts(g.get('by_kind'))})"),
              ("discovery", d.get("discovery")), ("governance", d.get("governance")), ("scope", "organisation" if d.get("organisation_wide") else "your workspaces")])
        if d.get("inbox"):
            i = d["inbox"]
            o.kv([("event inbox", f"{i.get('pending', 0)} pending, {i.get('failed', 0)} failed")])
        scans = d.get("last_scans") or []
        if scans:
            o.text("")
            o.table(scans, [("SOURCE", "source_id", 30), ("SCAN", "scan_id", 30), ("MODE", "mode"), ("STATUS", "status"), ("VERSION", "version"), ("FINISHED", "finished_at")])

    state.out.emit(state.client.intelligence.summary(), human)


def _counts(d: dict[str, Any] | None) -> str:
    return ", ".join(f"{k} {v}" for k, v in sorted((d or {}).items(), key=lambda kv: -int(kv[1]))) or "none"


# --------------------------------------------------------------------------------------------- search / assets
@intelligence_group.command("search", help="Find assets by name, path, description or alias (a case-insensitive substring).",
                            epilog="\b\nExamples:\n  olcf intelligence search sales\n  olcf intelligence search amount --type column --limit 20\n  olcf intelligence search revenue --domain Finance --classification PII")
@click.argument("query")
@click.option("--type", "type_", multiple=True, help="Asset type (repeatable): table, column, notebook, pipeline, semantic_model, report ...")
@click.option("--workspace-id", default=None)
@click.option("--domain", default=None, help="Governance domain.")
@click.option("--classification", default=None, help="Governance classification (e.g. PII).")
@click.option("--certification", default=None)
@click.option("--owner", default=None)
@click.option("--tag", default=None)
@click.option("--status", type=click.Choice(ASSET_STATUSES), default=None, help="Default: active assets.")
@paging_options
@json_option
@pass_state
def search_cmd(state: State, query: str, type_: tuple[str, ...], workspace_id: str | None, domain: str | None, classification: str | None, certification: str | None,
               owner: str | None, tag: str | None, status: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.intelligence.list_assets, limit=limit, fetch_all=fetch_all, q=query, type=list(type_) or None, workspace_id=workspace_id, domain=domain,
                     classification=classification, certification=certification, owner=owner, tag=tag, status=status)
    state.out.table(rows, ASSET_COLS, empty=f"nothing matches '{query}'")


@click.group("assets", help="List, inspect and trace assets: tables, columns, notebooks, pipelines, semantic models, reports.",
             epilog="\b\nExamples:\n  olcf intelligence assets list --type table\n  olcf intelligence assets show lakehouse_a/clean\n  olcf intelligence assets docs clean")
def assets_group() -> None:
    pass


@assets_group.command("list", help="List assets you can see, oldest first.",
                       epilog="\b\nExamples:\n  olcf intelligence assets list --type table,view --workspace-id wks_...\n  olcf intelligence assets list --all --json")
@click.option("-q", "--query", "q", default=None, help="Substring of name, path, description or alias.")
@click.option("--type", "type_", multiple=True, help="Asset type (repeatable or comma separated).")
@click.option("--workspace-id", default=None)
@click.option("--domain", default=None)
@click.option("--classification", default=None)
@click.option("--certification", default=None)
@click.option("--owner", default=None)
@click.option("--tag", default=None)
@click.option("--status", type=click.Choice(ASSET_STATUSES), default=None)
@paging_options
@json_option
@pass_state
def assets_list(state: State, q: str | None, type_: tuple[str, ...], workspace_id: str | None, domain: str | None, classification: str | None, certification: str | None,
                owner: str | None, tag: str | None, status: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.intelligence.list_assets, limit=limit, fetch_all=fetch_all, q=q, type=list(type_) or None, workspace_id=workspace_id, domain=domain,
                     classification=classification, certification=certification, owner=owner, tag=tag, status=status)
    state.out.table(rows, [*ASSET_COLS, ("UPDATED", "updated_at")], empty="no assets (run a scan: `olcf intelligence sources scan local --mode full`)")


@assets_group.command("show", help="One asset: facts, aliases, columns, lineage edge counts and open gaps.", epilog="\b\nExample:\n  olcf intelligence assets show lakehouse_a/clean")
@asset_argument
@json_option
@pass_state
def assets_show(state: State, asset: str, asset_type: str | None) -> None:
    doc = state.client.intelligence.get_asset(resolve_asset(state, asset, asset_type))

    def human(d: dict[str, Any]) -> None:
        o = state.out
        edges = d.get("lineage_edges") or {}
        o.kv([("id", d.get("id")), ("type", d.get("type")), ("name", d.get("name")), ("path", d.get("path")), ("workspace", d.get("workspace_id") or "(organisation)"),
              ("source", d.get("source_id")), ("status", f"{d.get('status')}" + (" (unresolved placeholder)" if d.get("placeholder") else "")), ("parent", (d.get("parent") or {}).get("name")),
              ("description", d.get("description")), ("tags", d.get("tags")), ("children", d.get("child_count")),
              ("lineage edges", f"{edges.get('upstream', 0)} upstream, {edges.get('downstream', 0)} downstream"), ("open gaps", d.get("open_gaps")),
              ("first seen", d.get("first_seen_scan")), ("last seen", d.get("last_seen_scan")), ("etag", doc.etag)])
        cols = (d.get("attrs") or {}).get("columns") or []
        if cols:
            o.text("")
            o.table([c if isinstance(c, dict) else {"name": str(c)} for c in cols], [("COLUMN", "name", 40), ("TYPE", "type", 30), ("DESCRIPTION", "description", 50)])
        if d.get("aliases"):
            o.text("")
            o.table(d["aliases"], [("ALIAS KIND", "kind"), ("VALUE", "value", 80)])

    state.out.emit(doc, human)


@assets_group.command("children", help="Child assets: the tables of a lakehouse, the columns of a table.", epilog="\b\nExample:\n  olcf intelligence assets children lakehouse_a --type table")
@asset_argument
@click.option("--child-type", "child_type", default=None, help="Only children of this type.")
@click.option("--status", type=click.Choice(ASSET_STATUSES), default=None)
@paging_options
@json_option
@pass_state
def assets_children(state: State, asset: str, asset_type: str | None, child_type: str | None, status: str | None, limit: int, fetch_all: bool) -> None:
    aid = resolve_asset(state, asset, asset_type)
    rows = all_pages(state.client.intelligence.children, limit=limit, fetch_all=fetch_all, asset_id=aid, type=child_type, status=status)
    state.out.table(rows, [("ID", "id", 30), ("TYPE", "type"), ("NAME", "name", 30), ("COLUMN TYPE", "attrs.type", 20), ("STATUS", "status")], empty="no children")


def _lineage_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--ids", "show_ids", is_flag=True, help="Show asset ids next to the names.")(f)
    f = click.option("--include-removed", is_flag=True, help="Also follow assets and edges that were removed from the source.")(f)
    f = click.option("--types", "types", multiple=True, help="Only these node types (repeatable); the others are bridged.")(f)
    f = click.option("--min-confidence", type=click.Choice(CONFIDENCES), default=None, help="Keep edges at least this strong.")(f)
    f = click.option("--column", default=None, help="Follow one column of the table (implies --level column).")(f)
    f = click.option("--level", type=click.Choice(["table", "column"]), default="table", show_default=True, help="Table lineage (a tree through the processes) or column lineage (a table).")(f)
    f = click.option("--depth", type=click.IntRange(1, 10), default=3, show_default=True, help="Dataset hops to follow.")(f)
    return click.option("--direction", type=click.Choice(["up", "down", "both"]), default="both", show_default=True, help="up = what feeds it, down = what it feeds.")(f)


def _table_names(state: State, doc: dict[str, Any]) -> dict[str, str]:
    names: dict[str, str] = {}
    for pid in R.parent_ids(doc)[:60]:
        try:
            names[pid] = str(state.client.intelligence.get_asset(pid).get("name"))
        except (NotFoundError, BadRequestError):
            continue
    return names


COLUMN_COLS = [("DIRECTION", "direction"), ("FROM", "from", 40), ("TO", "to", 40), ("VIA", "via", 24), ("TRANSFORMATION", "transformation", 50), ("CONFIDENCE", "confidence")]


def run_lineage(state: State, asset: str, asset_type: str | None, *, direction: str, depth: int, level: str, column: str | None, min_confidence: str | None, types: tuple[str, ...],
                include_removed: bool, show_ids: bool) -> None:
    aid = resolve_asset(state, asset, asset_type)
    doc = state.client.intelligence.lineage(aid, direction=direction, depth=depth, level="column" if column else level, min_confidence=min_confidence,  # type: ignore[arg-type]
                                            types=list(types) or None, column=column, include_removed=include_removed)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        if d.get("level") == "column":
            rows = R.column_rows(d, _table_names(state, d))
            root = d.get("root") or {}
            names = _table_names(state, d)
            subject = f"{root.get('name')}.{column}" if column else (f"{names.get(str(root.get('parent_id')), '?')}.{root.get('name')}" if root.get("type") == "column" else str(root.get("name")))
            o.text(f"Column lineage around {subject} ({len(rows)} edge(s), direction {d.get('direction')}, depth {d.get('depth')})")
            o.table(rows, COLUMN_COLS, empty="no column-level lineage known for this asset (column lineage is only derived where a transformation could be read)")
            for line in R._notes(d, False):
                o.text(line)
        else:
            for line in R.lineage_tree(d, style=conf_style(state), ids=show_ids):
                o.text(line)

    state.out.emit(doc, human)


@assets_group.command("lineage", help="Lineage around an asset (also available as `olcf intelligence lineage`).",
                      epilog="\b\nExamples:\n  olcf intelligence assets lineage lakehouse_a.clean\n  olcf intelligence assets lineage clean --direction up --depth 5")
@asset_argument
@_lineage_options
@json_option
@pass_state
def assets_lineage(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_lineage(state, asset, asset_type, **kw)


_LINEAGE_EPILOG = ("\b\nExamples:\n  olcf intelligence lineage lakehouse_a.clean\n  olcf intelligence lineage clean --direction up --depth 5 --min-confidence derived\n"
                   "  olcf intelligence lineage clean.amount --level column\n  olcf intelligence lineage sales_report --direction up --types table --json")


@intelligence_group.command("lineage", help="Lineage around an asset: a tree through notebooks, pipelines and queries (table level) or a table of column edges (--level column).",
                            epilog=_LINEAGE_EPILOG)
@asset_argument
@_lineage_options
@json_option
@pass_state
def lineage_cmd(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_lineage(state, asset, asset_type, **kw)


def run_origin(state: State, asset: str, asset_type: str | None, *, depth: int, min_confidence: str | None, level: str, column: str | None, show_ids: bool) -> None:
    aid = resolve_asset(state, asset, asset_type)
    doc = state.client.intelligence.origin(aid, depth=depth, min_confidence=min_confidence, level="column" if column else level, column=column)  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        o = state.out
        names = _table_names(state, d)
        a = d.get("asset") or {}
        a = {**a, "name": R.qualified(a, R.node_index(d), names)}
        if not d.get("has_upstream"):
            o.text(f"{R.label(a, ids=show_ids)} has no known upstream: it is itself a source, or nothing that feeds it has been discovered yet.")
        else:
            o.text(f"Where {R.label(a, ids=show_ids)} comes from ({len(d.get('roots') or [])} origin(s), {len(d.get('paths') or [])} path(s)):")
            o.table(R.origin_roots(d, names), [("ORIGIN", "name", 30), ("TYPE", "type"), ("PATH", "path", 36), ("WORKSPACE", "workspace_id", 28), ("SOURCE SYSTEM", "source"), ("PATHS", "paths"),
                                        ("CONFIDENCE", "confidence")])
            o.text("")
            o.text("Paths (source first; confidence is the weakest edge on the path):")
            for line in R.origin_chains(d, style=conf_style(state), table_names=names):
                o.text("  " + line)
        for line in R._notes(d, False):
            o.text(line)

    state.out.emit(doc, human)


def _origin_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--ids", "show_ids", is_flag=True, help="Show asset ids.")(f)
    f = click.option("--column", default=None, help="Trace one column of the table (implies --level column).")(f)
    f = click.option("--level", type=click.Choice(["table", "column"]), default="table", show_default=True)(f)
    f = click.option("--min-confidence", type=click.Choice(CONFIDENCES), default=None, help="Keep edges at least this strong.")(f)
    return click.option("--depth", type=click.IntRange(1, 10), default=10, show_default=True, help="Dataset hops to follow upstream.")(f)


_ORIGIN_EPILOG = "\b\nExamples:\n  olcf intelligence where-from sales_report\n  olcf intelligence where-from clean.amount --level column\n  olcf intelligence where-from revenue --min-confidence derived --json"


@assets_group.command("origin", help="Where does this come from: upstream roots and every path from them (also `olcf intelligence where-from`).", epilog=_ORIGIN_EPILOG)
@asset_argument
@_origin_options
@json_option
@pass_state
def assets_origin(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_origin(state, asset, asset_type, **kw)


@intelligence_group.command("where-from", help="Where does this asset originate? Lists the upstream roots, each path from them and its confidence.", epilog=_ORIGIN_EPILOG)
@asset_argument
@_origin_options
@json_option
@pass_state
def where_from_cmd(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_origin(state, asset, asset_type, **kw)


def run_impact(state: State, asset: str, asset_type: str | None, *, depth: int, min_confidence: str | None) -> None:
    aid = resolve_asset(state, asset, asset_type)
    doc = state.client.intelligence.impact(aid, depth=depth, min_confidence=min_confidence)  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        o = state.out
        a = d.get("asset") or {}
        by_type = {k: v.get("count", 0) for k, v in (d.get("by_type") or {}).items()}
        o.text(f"If {R.label(a)} changes, {d.get('total', 0)} downstream asset(s) are affected" + (f" ({_counts(by_type)})" if by_type else "") + ".")
        o.table(R.impact_rows(d), [("TYPE", "type"), ("NAME", "name", 32), ("PATH", "path", 36), ("WORKSPACE", "workspace_id", 28), ("HOPS", "hops"), ("CONFIDENCE", "confidence"),
                                   ("OWNERS", "owners", 30)], empty="nothing downstream is known to depend on it")
        procs = d.get("processes") or []
        if procs:
            o.text("Processes that read it: " + ", ".join(f"{p.get('name')} ({p.get('type')})" for p in procs[:20]))
        if not d.get("owners_known"):
            o.text("(owners are unknown: no governance owner data exists for these assets)")
        for line in R._notes(d, False):
            o.text(line)

    state.out.emit(doc, human)


def _impact_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--min-confidence", type=click.Choice(CONFIDENCES), default=None, help="Keep edges at least this strong.")(f)
    return click.option("--depth", type=click.IntRange(1, 10), default=5, show_default=True, help="Dataset hops to follow downstream.")(f)


_IMPACT_EPILOG = "\b\nExamples:\n  olcf intelligence impact raw\n  olcf intelligence impact clean.amount --depth 3\n  olcf intelligence impact lakehouse_a/raw --min-confidence derived --json"


@assets_group.command("impact", help="What breaks if this changes: downstream consumers by type (also `olcf intelligence impact`).", epilog=_IMPACT_EPILOG)
@asset_argument
@_impact_options
@json_option
@pass_state
def assets_impact(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_impact(state, asset, asset_type, **kw)


@intelligence_group.command("impact", help="Impact analysis: everything downstream that depends on this asset, grouped by type, with owners when known.", epilog=_IMPACT_EPILOG)
@asset_argument
@_impact_options
@json_option
@pass_state
def impact_cmd(state: State, asset: str, asset_type: str | None, **kw: Any) -> None:
    run_impact(state, asset, asset_type, **kw)


@assets_group.command("staleness", help="Is the data fresh? Last observed write, the run that wrote it, and the most upstream stale producer.",
                      epilog="\b\nExamples:\n  olcf intelligence assets staleness clean\n  olcf intelligence assets staleness mart --stale-after-hours 6 --stale-only")
@asset_argument
@click.option("--depth", type=click.IntRange(1, 10), default=5, show_default=True)
@click.option("--stale-after-hours", type=float, default=None, help="Freshness threshold (default: the platform setting, 24).")
@click.option("--stale-only", is_flag=True, help="List only the stale producers.")
@json_option
@pass_state
def assets_staleness(state: State, asset: str, asset_type: str | None, depth: int, stale_after_hours: float | None, stale_only: bool) -> None:
    doc = state.client.intelligence.staleness(resolve_asset(state, asset, asset_type), depth=depth, stale_after_hours=stale_after_hours)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.kv([("asset", R.label(d.get("asset"))), ("stale", "yes" if d.get("stale") else "no"), ("last write", d.get("last_write_at")),
              ("threshold", f"{d.get('threshold_hours')} h"), ("explanation", d.get("explanation"))])
        rows = R.staleness_rows(d, only_stale=stale_only)
        o.text("")
        o.table(rows, [("PRODUCER", "name", 30), ("TYPE", "type"), ("STALE", "stale"), ("ROOT CAUSE", "root_cause"), ("LAST WRITE", "last_write_at"), ("AGE H", "age_hours"),
                       ("LAST RUN", "last_run"), ("REASONS", "reasons", 50)], empty="nothing stale")
        for line in R._notes(d, False):
            o.text(line)

    state.out.emit(doc, human)


@assets_group.command("docs", help="Generated documentation (Markdown): description, schema, governance facts, lineage summary, freshness and schema changes.",
                      epilog="\b\nExamples:\n  olcf intelligence assets docs clean\n  olcf intelligence assets docs clean > clean.md\n  olcf intelligence assets docs clean --json")
@asset_argument
@json_option
@pass_state
def assets_docs(state: State, asset: str, asset_type: str | None) -> None:
    doc = state.client.intelligence.documentation(resolve_asset(state, asset, asset_type))
    state.out.emit(doc, lambda d: state.out.text(str(d.get("markdown") or "").rstrip()))


def _change_summary(ch: dict[str, Any] | None) -> str:
    ch = ch or {}
    bits = []
    if ch.get("created"):
        bits.append("created")
    if ch.get("columns_added"):
        bits.append("+" + ",+".join(map(str, ch["columns_added"])))
    if ch.get("columns_removed"):
        bits.append("-" + ",-".join(map(str, ch["columns_removed"])))
    for c in ch.get("columns_changed") or []:
        bits.append(f"{c.get('column')}: {c.get('from')} -> {c.get('to')}")
    other = sorted(set(ch) - {"created", "columns_added", "columns_removed", "columns_changed"})
    if other:
        bits.append("changed " + ", ".join(other))
    return "; ".join(bits) or "-"


@assets_group.command("history", help="Attribute versions of an asset, newest first, with what changed (columns added, removed, retyped).",
                      epilog="\b\nExamples:\n  olcf intelligence assets history raw\n  olcf intelligence assets history raw --schema-only")
@asset_argument
@click.option("--schema-only", is_flag=True, help="Only versions that changed the schema.")
@paging_options
@json_option
@pass_state
def assets_history(state: State, asset: str, asset_type: str | None, schema_only: bool, limit: int, fetch_all: bool) -> None:
    aid = resolve_asset(state, asset, asset_type)
    rows = all_pages(state.client.intelligence.history, limit=limit, fetch_all=fetch_all, asset_id=aid, schema_only=schema_only)
    for r in rows:
        r["summary"] = _change_summary(r.get("changes"))
    state.out.table(rows, [("WHEN", "created_at"), ("SCAN", "scan_id", 30), ("CHANGES", "summary", 100)], empty="no recorded versions")


@assets_group.command("sample", help="Sample rows of a table, masked and limited by the sampling policy (needs intelligence.sample; off until a policy allows it).",
                      epilog="\b\nExamples:\n  olcf intelligence assets sample lakehouse_a/clean --rows 5\n  olcf intelligence assets sample clean --column id --column amount\n\n"
                             "Sampling is audited and values are never stored. See `olcf intelligence sampling-policy`.")
@asset_argument
@click.option("--rows", type=click.IntRange(1, 100), default=None, help="Rows (policy maximum applies).")
@click.option("--column", "columns", multiple=True, help="Only these columns (repeatable); excluded columns stay excluded.")
@json_option
@pass_state
def assets_sample(state: State, asset: str, asset_type: str | None, rows: int | None, columns: tuple[str, ...]) -> None:
    doc = state.client.intelligence.sample(resolve_asset(state, asset, asset_type), rows=rows, columns=list(columns) or None)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        cols = [c["name"] for c in d.get("columns") or []]
        o.table(d.get("rows") or [], [(c, c, 40) for c in cols], empty="no rows", raw_headers=True)
        o.info(f"{d.get('row_count', 0)} row(s), masking {d.get('masking')}, policy scope {d.get('policy_scope')}"
               + (f", excluded columns: {', '.join(d['excluded_columns'])}" if d.get("excluded_columns") else ""))

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- snapshots / diff / unused
@intelligence_group.command("snapshots", help="Metadata snapshots (one per scan), newest first; their ids feed `diff`.", epilog="\b\nExample:\n  olcf intelligence snapshots --source local")
@click.option("--source", "source_id", default=None, help="Source id or name.")
@click.option("--status", type=click.Choice(SCAN_STATUSES), default=None)
@paging_options
@json_option
@pass_state
def snapshots_cmd(state: State, source_id: str | None, status: str | None, limit: int, fetch_all: bool) -> None:
    sid = state.client.intelligence.get_source(source_id)["id"] if source_id else None
    rows = all_pages(state.client.intelligence.snapshots, limit=limit, fetch_all=fetch_all, source_id=sid, status=status)
    names = source_names(state)
    for r in rows:
        st = r.get("stats") or {}
        r["source"] = names.get(str(r.get("source_id")), r.get("source_id"))
        r["finished"] = human_age(r.get("finished_at"))
        r["changes"] = f"+{st.get('assets_created', 0)} assets, {st.get('assets_updated', 0)} updated, +{st.get('edges_created', 0)} edges"
    state.out.table(rows, [("SNAPSHOT", "id", 30), ("SOURCE", "source", 24), ("VERSION", "version"), ("MODE", "mode"), ("STATUS", "status"), ("FINISHED", "finished"),
                           ("CHANGES", "changes", 50)], empty="no snapshots yet")


@intelligence_group.command("diff", help="What changed between two snapshots: assets and edges added, removed and changed (columns added, removed, retyped).",
                            epilog="\b\nExamples:\n  olcf intelligence diff scn_AAA scn_BBB\n  olcf intelligence diff --source local            the latest two snapshots of a source\n"
                                   "  olcf intelligence diff scn_AAA scn_BBB --json")
@click.argument("from_scan", required=False)
@click.argument("to_scan", required=False)
@click.option("--source", "source_id", default=None, help="Source id or name (to pick the latest two snapshots when none are given).")
@click.option("--limit", type=click.IntRange(1, 2000), default=500, show_default=True)
@json_option
@pass_state
def diff_cmd(state: State, from_scan: str | None, to_scan: str | None, source_id: str | None, limit: int) -> None:
    c = state.client.intelligence
    if bool(from_scan) != bool(to_scan):
        raise click.UsageError("give both snapshot ids, or none (then the latest two snapshots are compared)")
    if not from_scan or not to_scan:
        sid = c.get_source(source_id)["id"] if source_id else None
        done = [s for s in c.snapshots(source_id=sid, limit=50) if s.get("status") in ("succeeded", "partial")]
        if sid is None and len({s["source_id"] for s in done}) > 1:
            raise click.UsageError("several sources have snapshots; pass --source NAME or the two snapshot ids")
        if len(done) < 2:
            raise _not_found("fewer than two finished snapshots exist", "run another scan: `olcf intelligence sources scan SOURCE`")
        to_scan, from_scan = done[0]["id"], done[1]["id"]
    doc = c.diff(from_scan, to_scan, limit=limit)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        s = d.get("summary") or {}
        o.text(f"{d['from']['scan_id']} (v{d['from']['version']}) -> {d['to']['scan_id']} (v{d['to']['version']}) of source {d.get('source_id')}")
        o.text(f"assets: +{s.get('assets_added', 0)} added, -{s.get('assets_removed', 0)} removed, ~{s.get('assets_changed', 0)} changed;  "
               f"edges: +{s.get('edges_added', 0)} added, -{s.get('edges_removed', 0)} removed, ~{s.get('edges_changed', 0)} changed")
        a = d.get("assets") or {}
        for sign, key in (("+", "added"), ("-", "removed")):
            for x in a.get(key) or []:
                o.text(f"  {sign} {x.get('type')} {x.get('path') or x.get('name')}")
        for x in a.get("changed") or []:
            o.text(f"  ~ {x.get('type')} {x.get('path') or x.get('name')}: {_change_summary(x.get('changes'))}")
        e = d.get("edges") or {}
        for sign, key in (("+", "added"), ("-", "removed"), ("~", "changed")):
            for x in e.get(key) or []:
                o.text(f"  {sign} edge {x.get('src_name') or x.get('src')} {x.get('type')} {x.get('dst_name') or x.get('dst')} [{x.get('confidence')}]")
        if d.get("truncated"):
            o.text(f"(the lists were cut at --limit {limit})")

    state.out.emit(doc, human)


@intelligence_group.command("unused", help="Tables and views nothing was seen reading within N days (candidates for clean-up).",
                            epilog="\b\nExamples:\n  olcf intelligence unused --days 90\n  olcf intelligence unused --type table --workspace-id wks_... --all")
@click.option("--days", type=click.IntRange(0, 3650), default=None, help="Window in days (default: the platform setting, 30).")
@click.option("--type", "type_", default=None, help="table, view or both (comma separated).")
@click.option("--workspace-id", default=None)
@paging_options
@json_option
@pass_state
def unused_cmd(state: State, days: int | None, type_: str | None, workspace_id: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.intelligence.unused, limit=limit, fetch_all=fetch_all, days=days, type=type_, workspace_id=workspace_id)
    state.out.table(rows, [("ID", "id", 30), ("TYPE", "type"), ("NAME", "name", 30), ("PATH", "path", 40), ("WORKSPACE", "workspace_id", 28), ("FIRST SEEN", "first_seen_at"),
                           ("LAST READ", "last_read_at")], empty="no unused tables in that window")


# --------------------------------------------------------------------------------------------- sources
@click.group("sources", help="Discovery sources: the local platform, a Fabric tenant, a saved Power BI scanner result.",
             epilog="\b\nExamples:\n  olcf intelligence sources list\n  olcf intelligence sources add crm --kind fabric --tenant-id T --client-id C   (secret from $OLCF_FABRIC_CLIENT_SECRET or a prompt)\n"
                    "  olcf intelligence sources test crm\n  olcf intelligence sources scan crm --mode full --wait")
def sources_group() -> None:
    pass


@sources_group.command("list", help="Discovery sources you can see, with their last scan and next scan due.", epilog="\b\nExample:\n  olcf intelligence sources list --kind fabric")
@click.option("--kind", type=click.Choice(SOURCE_KINDS), default=None)
@json_option
@pass_state
def sources_list(state: State, kind: str | None) -> None:
    rows = [dict(r, status=r.get("last_status") or ("running" if r.get("running") else "")) for r in state.client.intelligence.list_sources(kind=kind)]
    state.out.table(rows, [("NAME", "name", 28), ("KIND", "kind"), ("ENABLED", "enabled"), ("STATUS", "status"), ("LAST SCAN", "last_scan_at"), ("NEXT DUE", "next_due_at"),
                           ("CREDENTIALS", "has_credentials"), ("ID", "id", 30)], empty="no sources")


@sources_group.command("show", help="One source with its (secret-free) configuration, schedule and scan state.", epilog="\b\nExample:\n  olcf intelligence sources show crm")
@click.argument("source")
@json_option
@pass_state
def sources_show(state: State, source: str) -> None:
    doc = state.client.intelligence.get_source(source)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.kv([("name", d.get("name")), ("id", d.get("id")), ("kind", d.get("kind")), ("enabled", d.get("enabled")), ("built-in", d.get("system")),
              ("credentials stored", d.get("has_credentials")), ("sampling identity", d.get("has_sampling_identity")), ("running", d.get("running")),
              ("last scan", f"{d.get('last_scan_id')} {d.get('last_status')} {human_age(d.get('last_scan_at'))}" if d.get("last_scan_id") else None),
              ("last full scan", d.get("last_full_at")), ("last incremental", d.get("last_incremental_at")), ("next due", d.get("next_due_at")),
              ("schedule", d.get("effective_schedule")), ("config", json.dumps(d.get("config") or {}, sort_keys=True)), ("version", d.get("version")), ("etag", doc.etag)])
        if d.get("import"):
            o.kv([("import", d["import"])])

    state.out.emit(doc, human)


def _read_file_secret(path: str) -> str:
    return read_input(path)


def _identity_credentials(which: str, secret_env: str, secret_stdin: bool, key_file: str | None, cert_file: str | None, prompt_label: str) -> dict[str, str]:
    """Credentials for one identity: a certificate (key + certificate files) or a client secret from the environment variable, stdin or a hidden prompt."""
    if bool(key_file) != bool(cert_file):
        raise click.UsageError(f"--{which}private-key-file and --{which}certificate-file go together")
    if key_file and cert_file:
        return {"private_key_pem": _read_file_secret(key_file), "certificate_pem": _read_file_secret(cert_file)}
    value = secret_prompt(prompt_label, from_env=secret_env, stdin=secret_stdin).strip()
    if not value:
        raise click.UsageError(f"no client secret given: set ${secret_env}, pass it on stdin with --{which}secret-stdin, or run interactively for a prompt")
    return {"client_secret": value}


@sources_group.command("add", help="Create a discovery source (local, catalog_mirror, fabric or scanner_import); credentials are write-only.",
                       epilog="\b\nExamples:\n  olcf intelligence sources add everything --kind local\n"
                              "  OLCF_FABRIC_CLIENT_SECRET=... olcf intelligence sources add crm --kind fabric --tenant-id T --client-id C \\\n"
                              "      --fabric-workspace GUID --incremental-every 900\n"
                              "  olcf intelligence sources add crm --kind fabric --connection-id con_... --test-only    test without saving\n"
                              "  olcf intelligence sources add pbi --kind scanner_import --file scanresult.json\n\n"
                              "The Fabric client secret comes from $OLCF_FABRIC_CLIENT_SECRET (or --secret-env NAME), stdin (--secret-stdin), a certificate (--private-key-file and\n"
                              "--certificate-file) or a hidden prompt; it is never accepted as an argument and never printed. Use --sampling-client-id to give data sampling its own\n"
                              "identity (the scanner itself never reads data).")
@click.argument("name")
@click.option("--kind", type=click.Choice(SOURCE_KINDS), required=True)
@click.option("--workspace", "workspaces", multiple=True, help="local / catalog_mirror: OLCF workspace ids or names to scan (default all).")
@click.option("--value-profiling/--no-value-profiling", default=None, help="local: profile values (reads data; needs a sampling policy).")
@click.option("--tenant-id", default=None, help="fabric: Entra tenant id (GUID or domain).")
@click.option("--client-id", default=None, help="fabric: application (client) id of the metadata service principal.")
@click.option("--connection-id", default=None, help="fabric: use an existing service-principal connection (con_...) instead of --client-id.")
@click.option("--secret-env", default="OLCF_FABRIC_CLIENT_SECRET", show_default=True, help="fabric: environment variable holding the client secret.")
@click.option("--secret-stdin", is_flag=True, help="fabric: read the client secret from the first line of stdin.")
@click.option("--private-key-file", type=click.Path(exists=True, dir_okay=False), default=None, help="fabric: PEM private key (certificate authentication).")
@click.option("--certificate-file", type=click.Path(exists=True, dir_okay=False), default=None, help="fabric: PEM certificate matching the private key.")
@click.option("--fabric-workspace", "fabric_workspaces", multiple=True, help="fabric: limit to these Fabric workspace ids (default all the identity can see).")
@click.option("--item-type", "item_types", multiple=True, help="fabric: limit to these item types (Lakehouse, Notebook, DataPipeline, SemanticModel, Report ...).")
@click.option("--sampling-client-id", default=None, help="fabric: separate service principal for data sampling.")
@click.option("--sampling-tenant-id", default=None, help="fabric: tenant of the sampling identity (default: --tenant-id).")
@click.option("--sampling-secret-env", default="OLCF_FABRIC_SAMPLING_CLIENT_SECRET", show_default=True, help="fabric: environment variable holding the sampling client secret.")
@click.option("--sampling-private-key-file", type=click.Path(exists=True, dir_okay=False), default=None)
@click.option("--sampling-certificate-file", type=click.Path(exists=True, dir_okay=False), default=None)
@click.option("--file", "scanner_file", type=click.Path(dir_okay=False), default=None, help="scanner_import: the saved scanner result (JSON; - for stdin).")
@click.option("--no-scan", is_flag=True, help="scanner_import: store the document without scanning it yet.")
@click.option("--config", "config_pairs", multiple=True, help="Extra config KEY=VALUE (JSON values), repeatable.")
@click.option("--config-file", type=click.Path(exists=True, dir_okay=False), default=None, help="Extra config as a JSON file.")
@click.option("--incremental-every", type=int, default=None, help="Seconds between incremental scans (0 = never; 60..2592000).")
@click.option("--full-every", type=int, default=None, help="Seconds between full scans (0 = never; 60..2592000).")
@click.option("--disabled", is_flag=True, help="Create the source disabled (no scheduled scans).")
@click.option("--test-only", is_flag=True, help="Run the connectivity test for this configuration and exit without saving anything.")
@json_option
@pass_state
def sources_add(state: State, name: str, kind: str, workspaces: tuple[str, ...], value_profiling: bool | None, tenant_id: str | None, client_id: str | None,
                connection_id: str | None, secret_env: str, secret_stdin: bool, private_key_file: str | None, certificate_file: str | None, fabric_workspaces: tuple[str, ...],
                item_types: tuple[str, ...], sampling_client_id: str | None, sampling_tenant_id: str | None, sampling_secret_env: str, sampling_private_key_file: str | None,
                sampling_certificate_file: str | None, scanner_file: str | None, no_scan: bool, config_pairs: tuple[str, ...], config_file: str | None,
                incremental_every: int | None, full_every: int | None, disabled: bool, test_only: bool) -> None:
    c = state.client.intelligence
    config: dict[str, Any] = {}
    if config_file:
        loaded = json.loads(read_input(config_file))
        if not isinstance(loaded, dict):
            raise click.BadParameter("--config-file must contain a JSON object")
        config.update(loaded)
    config.update(parse_kv(config_pairs))
    credentials: dict[str, str] | None = None
    sampling_credentials: dict[str, str] | None = None
    if kind in ("local", "catalog_mirror") and workspaces:
        config["workspace_ids"] = list(workspaces)
    if kind == "local" and value_profiling is not None:
        config["value_profiling"] = value_profiling
    if kind == "fabric":
        if connection_id:
            config["metadata_identity"] = {"connection_id": connection_id}
        elif tenant_id and client_id:
            config["metadata_identity"] = {"tenant_id": tenant_id, "client_id": client_id}
        elif "metadata_identity" not in config:
            raise click.UsageError("a fabric source needs --connection-id, or --tenant-id and --client-id (the secret is read from the environment or a prompt)")
        if tenant_id:
            config["tenant_id"] = tenant_id
        if fabric_workspaces:
            config["workspace_ids"] = list(fabric_workspaces)
        if item_types:
            config["item_types"] = list(item_types)
        if "connection_id" not in config["metadata_identity"] and "secret_id" not in config["metadata_identity"]:
            credentials = _identity_credentials("", secret_env, secret_stdin, private_key_file, certificate_file, "Client secret of the metadata service principal")
        if sampling_client_id:
            stenant = sampling_tenant_id or tenant_id
            if not stenant:
                raise click.UsageError("--sampling-client-id needs --sampling-tenant-id (or --tenant-id)")
            config["sampling_identity"] = {"tenant_id": stenant, "client_id": sampling_client_id}
            sampling_credentials = _identity_credentials("sampling-", sampling_secret_env, False, sampling_private_key_file, sampling_certificate_file,
                                                         "Client secret of the sampling service principal")
    elif kind == "scanner_import":
        if not scanner_file:
            raise click.UsageError("a scanner_import source needs --file RESULT.json (the saved Power BI admin scanner result)")
        if test_only:
            raise click.UsageError("--test-only does not apply to scanner_import (nothing to connect to); `olcf intelligence import scanner` validates the file")
    schedule = {k: v for k, v in (("incremental_seconds", incremental_every), ("full_seconds", full_every)) if v is not None}
    if test_only:
        result = c.test_config(kind, config=config, name=name, credentials=credentials, sampling_credentials=sampling_credentials)
        _report_test(state, result)
        return
    if kind == "scanner_import":
        res = c.import_scanner(read_input(scanner_file, binary=True), name=name, tenant_id=config.get("tenant_id"), scan=not no_scan)
        state.out.emit(res, _import_human(state))
        return
    src = c.create_source(name, kind, config=config, schedule=schedule or None, enabled=not disabled, credentials=credentials, sampling_credentials=sampling_credentials)

    def human(d: dict[str, Any]) -> None:
        state.out.success(f"created {d['kind']} source {d['name']} ({d['id']})")
        if d["kind"] == "fabric":
            state.out.info(f"next: `olcf intelligence sources test {d['name']}` to check each connectivity step, then `olcf intelligence sources scan {d['name']} --mode full --wait`")

    state.out.emit(src, human)


def _import_human(state: State) -> Callable[[dict[str, Any]], None]:
    """One line for an imported scanner result: how many workspaces and data source instances went into which source, and the scan that followed (if any)."""
    def human(d: dict[str, Any]) -> None:
        scan = d.get("scan") or {}
        state.out.success(f"imported {d.get('workspaces')} workspace(s), {d.get('datasource_instances')} data source instance(s) into {d['source']['name']} ({d['source']['id']})"
                          + (f"; scan {scan.get('id')} {scan.get('status')}" if scan else ""))

    return human


def _report_test(state: State, result: dict[str, Any]) -> None:
    """Print a connectivity test; the exit status is 1 when any step failed."""
    def human(d: dict[str, Any]) -> None:
        o = state.out
        for line in R.step_lines(d, style=conf_style(state)):
            o.text(line)
        o.text("")
        o.text(o.style("connectivity OK", fg="green", bold=True) if d.get("ok") else o.style(f"connectivity FAILED at step: {d.get('failed_step')}", fg="red", bold=True))

    state.out.emit(result, human)
    if not result.get("ok"):
        raise SystemExit(EXIT_FAIL)


@sources_group.command("test", help="Test a source step by step (credentials, tokens, listing, permissions, sampling identity); exits non-zero on the first failing step.",
                       epilog="\b\nExamples:\n  olcf intelligence sources test crm\n  olcf intelligence sources test crm --json\n\n"
                              "Each step prints ok/FAIL with a detail and, for a failure, a `fix:` line. Steps after the first failure are not run.")
@click.argument("source")
@json_option
@pass_state
def sources_test(state: State, source: str) -> None:
    _report_test(state, state.client.intelligence.test_source(source))


@sources_group.command("scan", help="Start a scan of a source (incremental by default); --wait follows it until it ends.",
                       epilog="\b\nExamples:\n  olcf intelligence sources scan local --mode full --wait\n  olcf intelligence sources scan crm --workspace-id GUID --wait --timeout 1800\n\n"
                              "A limited (--workspace-id) full scan never removes assets. Exit status: 0 succeeded or partial, 1 failed or cancelled, 9 timed out waiting.")
@click.argument("source")
@click.option("--mode", type=click.Choice(["full", "incremental"]), default="incremental", show_default=True)
@click.option("--workspace-id", "workspace_ids", multiple=True, help="Limit the scan to these workspace ids (repeatable).")
@click.option("--wait/--no-wait", default=False, help="Follow the scan until it ends.")
@click.option("--timeout", type=float, default=900.0, show_default=True, help="Seconds to wait with --wait.")
@json_option
@pass_state
def sources_scan(state: State, source: str, mode: str, workspace_ids: tuple[str, ...], wait: bool, timeout: float) -> None:
    c = state.client.intelligence
    scan = c.scan_source(source, mode=mode, workspace_ids=list(workspace_ids) or None)  # type: ignore[arg-type]
    state.out.step(f"scan {scan.get('id')} started ({mode})")
    if wait:
        try:
            scan = c.wait_scan(scan["id"], timeout=timeout)
        except OlcfTimeoutError:
            state.out.emit(c.get_scan(scan["id"]), _scan_human(state))
            state.out.error(f"scan {scan['id']} is still running after {timeout:g}s", f"follow it with `olcf intelligence scans show {scan['id']}`")
            raise SystemExit(EXIT_TIMEOUT) from None
    state.out.emit(scan, _scan_human(state))
    if scan.get("status") in ("failed", "cancelled"):
        raise SystemExit(EXIT_FAIL)


@sources_group.command("delete", help="Delete a source; assets only it reported are marked removed (history is kept). The built-in local source cannot be deleted.",
                       epilog="\b\nExample:\n  olcf intelligence sources delete crm --yes")
@click.argument("source")
@yes_option
@json_option
@pass_state
def sources_delete(state: State, source: str, yes: bool) -> None:
    confirm(state, f"Delete discovery source {source}? Assets only it reported are marked removed.", yes)
    res = state.client.intelligence.delete_source(source)
    state.out.emit(res, lambda d: state.out.success(f"deleted source {source}" + (f" ({d.get('assets_removed')} asset(s) marked removed)" if d.get("assets_removed") is not None else "")))


# --------------------------------------------------------------------------------------------- scans
def _gap_summary(gaps: Any) -> str:
    """``3 (permission_denied 2, dynamic_reference 1)`` from a scan's ``{"total", "by_kind"}`` (or ``none``)."""
    if isinstance(gaps, dict):
        total = gaps.get("total", 0)
        return f"{total}" + (f" ({_counts(gaps.get('by_kind'))})" if total else "")
    return str(gaps if gaps is not None else "none")


def _scan_human(state: State) -> Callable[[dict[str, Any]], None]:
    def human(d: dict[str, Any]) -> None:
        o = state.out
        st = d.get("stats") or {}
        ap = st.get("apply") or st
        o.kv([("scan", d.get("id")), ("source", d.get("source_id")), ("mode", d.get("mode")), ("trigger", d.get("trigger")), ("status", o.state(str(d.get("status")))),
              ("version", d.get("version")), ("started", d.get("started_at")), ("finished", d.get("finished_at")), ("error", d.get("error")),
              ("assets", f"{ap.get('assets_created', 0)} created, {ap.get('assets_updated', 0)} updated, {ap.get('assets_unchanged', 0)} unchanged"),
              ("edges", f"{ap.get('edges_created', 0)} created, {ap.get('edges_updated', 0)} updated, {ap.get('edges_unchanged', 0)} unchanged"),
              ("gaps", _gap_summary(d.get("gaps")))])
        sweep = {k: v for k, v in (st.get("sweep") or {}).items() if v}
        if sweep:
            o.kv([("removal sweep", ", ".join(f"{k.replace('_', ' ')} {v}" for k, v in sweep.items()))])
        if ap.get("error_samples"):
            o.text("errors: " + "; ".join(map(str, ap["error_samples"][:5])))

    return human


@click.group("scans", help="Scans (metadata snapshots) of the discovery sources.", epilog="\b\nExamples:\n  olcf intelligence scans list --status failed\n  olcf intelligence scans show scn_...")
def scans_group() -> None:
    pass


@scans_group.command("list", help="Scans of the sources you can see, newest first.", epilog="\b\nExample:\n  olcf intelligence scans list --source crm --status partial")
@click.option("--source", "source_id", default=None, help="Source id or name.")
@click.option("--status", type=click.Choice(SCAN_STATUSES), default=None)
@paging_options
@json_option
@pass_state
def scans_list(state: State, source_id: str | None, status: str | None, limit: int, fetch_all: bool) -> None:
    sid = state.client.intelligence.get_source(source_id)["id"] if source_id else None
    rows = all_pages(state.client.intelligence.list_scans, limit=limit, fetch_all=fetch_all, source_id=sid, status=status)
    names = source_names(state)
    for r in rows:
        r["source"] = names.get(str(r.get("source_id")), r.get("source_id"))
        r["started"], r["finished"] = human_age(r.get("started_at")), human_age(r.get("finished_at"))
    state.out.table(rows, [("SCAN", "id", 30), ("SOURCE", "source", 24), ("MODE", "mode"), ("TRIGGER", "trigger"), ("STATUS", "status"), ("VERSION", "version"),
                           ("STARTED", "started"), ("FINISHED", "finished")], empty="no scans yet")


@scans_group.command("show", help="One scan: status, statistics and gaps by kind.", epilog="\b\nExample:\n  olcf intelligence scans show scn_01...")
@click.argument("scan_id")
@json_option
@pass_state
def scans_show(state: State, scan_id: str) -> None:
    state.out.emit(state.client.intelligence.get_scan(scan_id), _scan_human(state))


@scans_group.command("cancel", help="Ask a running scan to stop (it ends cancelled after the batch in progress).", epilog="\b\nExample:\n  olcf intelligence scans cancel scn_01...")
@click.argument("scan_id")
@json_option
@pass_state
def scans_cancel(state: State, scan_id: str) -> None:
    res = state.client.intelligence.cancel_scan(scan_id)
    state.out.emit(res, lambda d: state.out.success(f"cancellation requested for {scan_id}"))


# --------------------------------------------------------------------------------------------- gaps / coverage
@intelligence_group.command("gaps", help="What discovery or analysis could not read (permission denied, API unavailable, dynamic references ...) and how to fix it.",
                            epilog="\b\nExamples:\n  olcf intelligence gaps\n  olcf intelligence gaps --kind permission_denied --source crm\n  olcf intelligence gaps --asset etl --resolved")
@click.option("--source", "source_id", default=None, help="Source id or name.")
@click.option("--scan", "scan_id", default=None)
@click.option("--asset", default=None, help="Only gaps of this asset.")
@click.option("--kind", default=None, help="permission_denied, api_unavailable, dynamic_reference, unsupported_construct, parse_error, ...")
@click.option("--resolved", is_flag=True, help="Show resolved gaps instead of open ones.")
@paging_options
@json_option
@pass_state
def gaps_cmd(state: State, source_id: str | None, scan_id: str | None, asset: str | None, kind: str | None, resolved: bool, limit: int, fetch_all: bool) -> None:
    c = state.client.intelligence
    sid = c.get_source(source_id)["id"] if source_id else None
    rows = all_pages(c.list_gaps, limit=limit, fetch_all=fetch_all, source_id=sid, scan_id=scan_id, asset_id=resolve_asset(state, asset) if asset else None, kind=kind,
                     resolved=resolved)
    names = source_names(state)
    for r in rows:
        r["source"] = names.get(str(r.get("source_id")), r.get("source_id"))
        r["seen"] = human_age(r.get("last_seen_at"))
    state.out.table(rows, [("KIND", "kind", 24), ("SUBJECT", "subject", 28), ("DETAIL", "detail", 56), ("FIX", "remediation", 56), ("SOURCE", "source", 20),
                           ("LAST SEEN", "seen")], empty="no open gaps" if not resolved else "no resolved gaps")


@intelligence_group.command("coverage", help="Per source: assets by type, open gaps, last scan and next scan due; and whether sampling is on.",
                            epilog="\b\nExample:\n  olcf intelligence coverage")
@json_option
@pass_state
def coverage_cmd(state: State) -> None:
    doc = state.client.intelligence.coverage()

    def human(d: dict[str, Any]) -> None:
        o = state.out
        rows = [{"name": s.get("name"), "kind": s.get("kind"), "enabled": s.get("enabled"), "assets": (s.get("assets") or {}).get("active", 0),
                 "by_type": _counts((s.get("assets") or {}).get("by_type")), "gaps": (s.get("open_gaps") or {}).get("total", 0), "gap_kinds": _counts((s.get("open_gaps") or {}).get("by_kind")),
                 "status": s.get("last_status") or "", "last_full_at": s.get("last_full_at"), "next_due_at": s.get("next_due_at")} for s in d.get("sources") or []]
        o.table(rows, [("SOURCE", "name", 24), ("KIND", "kind"), ("ENABLED", "enabled"), ("ASSETS", "assets"), ("BY TYPE", "by_type", 40), ("OPEN GAPS", "gaps"), ("GAP KINDS", "gap_kinds", 36),
                       ("STATUS", "status"), ("LAST FULL", "last_full_at"), ("NEXT DUE", "next_due_at")], empty="no sources")
        s = d.get("sampling") or {}
        o.text("")
        o.text(f"sampling: {'ON' if s.get('enabled') else 'off'}" + (f" - {s['note']}" if s.get("note") else ""))
        o.text("connectors: " + ", ".join(d.get("connectors") or []))

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- import
@click.group("import", help="Import metadata from files.", epilog="\b\nExample:\n  olcf intelligence import scanner scanresult.json --name 'Contoso tenant'")
def import_group() -> None:
    pass


@import_group.command("scanner", help="Import a saved Power BI admin scanner result (JSON) as a scanner_import source and scan it.",
                      epilog="\b\nExamples:\n  olcf intelligence import scanner scanresult.json --name 'Contoso tenant' --wait\n  cat scanresult.json | olcf intelligence import scanner - --no-scan\n\n"
                             "The file is the JSON body of GET /v1.0/myorg/admin/workspaces/scanResult/{scanId}. It is validated and refused if it looks like it contains a credential.\n"
                             "Importing again under the same --name replaces the stored document.")
@click.argument("file", type=click.Path(dir_okay=False, allow_dash=True))
@click.option("--name", default=None, help="Source name (default 'Power BI scanner import').")
@click.option("--tenant-id", default=None)
@click.option("--workspace", "workspace_id", default=None, help="OLCF workspace the imported assets belong to.")
@click.option("--no-scan", is_flag=True, help="Store the document without scanning it.")
@click.option("--wait", is_flag=True, help="Hold until the scan has ended.")
@json_option
@pass_state
def import_scanner(state: State, file: str, name: str | None, tenant_id: str | None, workspace_id: str | None, no_scan: bool, wait: bool) -> None:
    data = read_input(file, binary=True)
    res = state.client.intelligence.import_scanner(data, name=name, tenant_id=tenant_id, workspace_id=workspace_id, scan=not no_scan, wait=wait)

    state.out.emit(res, _import_human(state))


# --------------------------------------------------------------------------------------------- sampling policies
@click.group("sampling-policy", help="Sampling policies: how many rows (0..100), which masking and whether classified columns are excluded. Off by default.",
             epilog="\b\nExamples:\n  olcf intelligence sampling-policy list\n  olcf intelligence sampling-policy set --scope workspace --scope-id analytics --max-rows 5 --masking hash\n"
                    "  olcf intelligence sampling-policy delete smp_...\n\nThe narrowest scope wins (asset, source, workspace, global). Sampling is off until a policy with max_rows > 0 exists.")
def policy_group() -> None:
    pass


@policy_group.command("list", help="Sampling policies you manage (none means nothing is ever sampled).", epilog="\b\nExample:\n  olcf intelligence sampling-policy list --scope workspace")
@click.option("--scope", "scope_type", type=click.Choice(["global", "workspace", "source", "asset"]), default=None)
@json_option
@pass_state
def policy_list(state: State, scope_type: str | None) -> None:
    rows = state.client.intelligence.list_sampling_policies(scope_type=scope_type)  # type: ignore[arg-type]
    state.out.table(rows, [("ID", "id", 30), ("SCOPE", "scope_type"), ("SCOPE ID", "scope_id", 30), ("MAX ROWS", "max_rows"), ("MASKING", "masking"),
                           ("EXCLUDE CLASSIFIED", "exclude_classified"), ("ENABLED", "enabled"), ("UPDATED", "updated_at")], empty="no sampling policies: sampling is off everywhere")


@policy_group.command("set", help="Create or replace the sampling policy of one scope.",
                       epilog="\b\nExamples:\n  olcf intelligence sampling-policy set --scope global --max-rows 10 --masking partial\n"
                              "  olcf intelligence sampling-policy set --scope asset --scope-id lakehouse_a/clean --max-rows 0     turn sampling off for one table\n"
                              "  olcf intelligence sampling-policy set --scope workspace --scope-id analytics --max-rows 5 --masking hash --include-classified")
@click.option("--scope", "scope_type", type=click.Choice(["global", "workspace", "source", "asset"]), required=True)
@click.option("--scope-id", default=None, help="Workspace name or id / source name or id / asset reference (not for global).")
@click.option("--max-rows", type=click.IntRange(0, 100), required=True, help="Rows a sample may return; 0 turns sampling off for the scope.")
@click.option("--masking", type=click.Choice(["none", "hash", "redact", "partial"]), default="partial", show_default=True)
@click.option("--exclude-classified/--include-classified", default=True, show_default=True, help="Keep classified and sensitive-looking columns out of samples.")
@click.option("--disabled", is_flag=True, help="Store the policy switched off.")
@json_option
@pass_state
def policy_set(state: State, scope_type: str, scope_id: str | None, max_rows: int, masking: str, exclude_classified: bool, disabled: bool) -> None:
    c = state.client
    if scope_type == "global":
        if scope_id:
            raise click.UsageError("the global scope takes no --scope-id")
        sid = None
    else:
        if not scope_id:
            raise click.UsageError(f"--scope {scope_type} needs --scope-id")
        sid = (scope_id if scope_id.startswith("wks_") else c.workspaces.get(scope_id)["id"]) if scope_type == "workspace" else (
            c.intelligence.get_source(scope_id)["id"] if scope_type == "source" else resolve_asset(state, scope_id))
    res = c.intelligence.put_sampling_policy(scope_type, max_rows, scope_id=sid, masking=masking, exclude_classified=exclude_classified, enabled=not disabled)  # type: ignore[arg-type]
    state.out.emit(res, lambda d: state.out.success(f"{scope_type} sampling policy {d.get('id')}: up to {d.get('max_rows')} row(s), masking {d.get('masking')}"
                                                     + (", classified columns excluded" if d.get("exclude_classified") else "")))


@policy_group.command("delete", help="Remove a sampling policy (the scope falls back to the next wider policy, and finally to off).",
                      epilog="\b\nExample:\n  olcf intelligence sampling-policy delete smp_01... --yes")
@click.argument("policy_id")
@yes_option
@json_option
@pass_state
def policy_delete(state: State, policy_id: str, yes: bool) -> None:
    confirm(state, f"Delete sampling policy {policy_id}?", yes)
    res = state.client.intelligence.delete_sampling_policy(policy_id)
    state.out.emit(res, lambda d: state.out.success(f"deleted sampling policy {policy_id}"))


# --------------------------------------------------------------------------------------------- inbox
@click.group("inbox", help="The run-time event inbox that feeds verified lineage (operators).", epilog="\b\nExample:\n  olcf intelligence inbox requeue")
def inbox_group() -> None:
    pass


@inbox_group.command("requeue", help="Queue every event the inbox gave up on (status failed) for another set of attempts. Failed events are also retried once on their own.",
                     epilog="\b\nExample:\n  olcf intelligence inbox requeue\n\nNeeds the intelligence.manage permission.")
@json_option
@pass_state
def inbox_requeue(state: State) -> None:
    res = state.client.intelligence.requeue_inbox()

    def human(d: dict[str, Any]) -> None:
        box = d.get("inbox") or {}
        state.out.success(f"{d.get('requeued', 0)} failed event(s) queued again")
        state.out.kv([("pending", box.get("pending", 0)), ("done", box.get("done", 0)), ("failed", box.get("failed", 0)), ("skipped", box.get("skipped", 0))])

    state.out.emit(res, human)


def register(group: click.Group) -> None:
    for sub in (assets_group, sources_group, scans_group, import_group, policy_group, inbox_group):
        intelligence_group.add_command(sub)
    group.add_command(intelligence_group)

