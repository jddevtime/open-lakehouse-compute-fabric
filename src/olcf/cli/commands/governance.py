# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf governance`` - the business glossary, domains, data products, owners, classifications, the review queue, certification, change requests, policies and quality.

Everything here is a thin layer over :class:`olcf.sdk.Client` ``.governance`` (``/v1/governance/*``); the terminal renderers live in :mod:`olcf.cli.gov_render`.

* Governance **observes and reports**: a policy never blocks a job, a query or a pipeline, and ``--mode enforce`` is refused by the server.
* Objects are addressed by id (``trm_...``, ``dom_...``, ``dpr_...``, ``cls_...``, ``plc_...``) or by name; assets by id, name, path or dotted name exactly like
  ``olcf intelligence``.  A name that matches several objects is refused with the candidates listed (exit 7).
* Changing commands that edit one object take ``--if-match ETAG`` (the ``etag`` printed by ``show``): the change is refused with exit 6 when somebody else changed the object.
* Every command accepts ``--json`` (and the global ``-o json|yaml|csv``); messages go to stderr, data to stdout; exit codes are the documented ones (``olcf --help``).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import click

from olcf.cli import gov_render as G
from olcf.cli.commands.intelligence import all_pages, json_option, paging_options, resolve_asset
from olcf.cli.common import confirm, parse_kv, pass_state, read_input, yes_option
from olcf.cli.state import EXIT_FAIL, State
from olcf.sdk import BadRequestError, NotFoundError

HELP_FOOTER = "\n\nAll commands take --json (or the global -o json|yaml|csv) and use the documented exit codes (see `olcf --help`)."
OWNER_ROLES = ["owner", "steward", "custodian", "contact"]
SUBJECT_KINDS = ["asset", "domain", "product", "term"]
CERT_KINDS = ["asset", "product", "term"]
CERT_STATES = ["none", "draft", "certified", "deprecated"]
SEVERITIES = ["low", "medium", "high", "critical"]
CHANGE_TYPES = ["certify", "deprecate", "classification_change", "term_link", "ownership_change", "policy_exception"]
TERM_STATUSES = ["draft", "approved", "deprecated"]
PRODUCT_STATUSES = ["draft", "published", "deprecated"]
CONFIDENCES = ["verified", "derived", "inferred", "unknown"]
_PREFIX = {"term": "trm_", "domain": "dom_", "product": "dpr_", "policy": "plc_", "classification": "cls_"}


# --------------------------------------------------------------------------------------------- shared plumbing
def if_match_option(f: Callable[..., Any]) -> Callable[..., Any]:
    """``--if-match ETAG``: refuse (exit 6) unless the object still has this etag, so a concurrent change is not overwritten."""
    return click.option("--if-match", "if_match", default=None, metavar="ETAG", help="Refuse unless the object still has this etag (see `show`).")(f)


def _candidate(c: dict[str, Any]) -> str:
    return f"{c.get('id')}  {c.get('name')}" + (f"  ({c.get('glossary')})" if c.get("glossary") else "")


def resolve_named(state: State, kind: str, ref: str) -> str:
    """The id of a term, domain, data product, classification or policy given as an id or an exact (case-insensitive) name.

    Raises :class:`~olcf.sdk.NotFoundError` (exit 5) when nothing matches and :class:`~olcf.sdk.BadRequestError` (exit 7, candidates listed) when several objects share the name.
    """
    ref = ref.strip()
    if ref.startswith(_PREFIX[kind]):
        return ref
    g = state.client.governance
    if kind == "term":
        pool = [h["term"] for h in g.search_glossary(ref, limit=100, include_deprecated=True)]
    elif kind == "domain":
        pool = list(g.list_domains(q=ref, limit=200))
    elif kind == "product":
        pool = list(g.list_products(q=ref, limit=200))
    elif kind == "classification":
        pool = list(g.list_classifications())
    else:
        pool = all_pages(g.list_policies, limit=500, fetch_all=True)
    hits = [c for c in pool if str(c.get("name") or "").lower() == ref.lower()]
    if not hits and kind == "term":                                 # an abbreviation or synonym that matches exactly is a name too
        hits = [h["term"] for h in g.search_glossary(ref, limit=100, include_deprecated=True) if h.get("score", 0) >= 80]
    if not hits:
        plural = {"classification": "classifications", "policy": "policies"}.get(kind, kind + "s")
        raise NotFoundError(f"no {kind} is named '{ref}'", status=404, code="not_found", remediation=f"list them with `olcf governance {plural} list`, or pass the id ({_PREFIX[kind]}...)")
    if len(hits) > 1:
        shown = "\n  ".join(_candidate(c) for c in hits[:12])
        raise BadRequestError(f"'{ref}' names {len(hits)} {kind}s:\n  {shown}", status=400, code="ambiguous_asset", remediation=f"pass the id ({_PREFIX[kind]}...)")
    return str(hits[0]["id"])


def column_labels(state: State, *docs: Any, limit: int = 40) -> dict[str, str]:
    """``{column id: "table.column"}`` for the columns mentioned in ``docs`` (a column reference carries no path, and ``amount -> amount`` says nothing).

    At most ``limit`` columns are looked up; a column that cannot be read keeps its bare name.
    """
    out: dict[str, str] = {}
    for ref in G.column_refs(*docs)[:limit]:
        try:
            parent = (state.client.intelligence.get_asset(ref["id"]).get("parent") or {}).get("name")
        except (NotFoundError, BadRequestError):
            continue
        if parent:
            out[str(ref["id"])] = f"{parent}.{ref.get('name')}"
    return out


def resolve_subject(state: State, kind: str, ref: str, asset_type: str | None = None) -> str:
    """The id of an asset (any reference accepted by ``olcf intelligence``) or of a named governance object."""
    return resolve_asset(state, ref, asset_type) if kind == "asset" else resolve_named(state, kind, ref)


def _load_json_object(text: str | None, path: str | None, pairs: tuple[str, ...], what: str) -> dict[str, Any]:
    """A JSON object from ``--<what> JSON``, ``--<what>-file FILE|-`` and ``--set key=value`` pairs (later sources win)."""
    out: dict[str, Any] = {}
    for source, label in ((text, f"--{what}"), (read_input(path) if path else None, f"--{what}-file")):
        if source:
            try:
                loaded = json.loads(source)
            except ValueError as exc:
                raise click.BadParameter(f"{label} is not valid JSON: {exc}") from None
            if not isinstance(loaded, dict):
                raise click.BadParameter(f"{label} must be a JSON object")
            out.update(loaded)
    out.update(parse_kv(pairs))
    return out


def _owner_body(role: str, user: str | None, name: str | None, email: str | None) -> dict[str, Any]:
    if bool(user) == bool(name):
        raise click.UsageError("give the owner as --user USER (an OLCF user: id, e-mail or user name) or as --name NAME [--email E-MAIL] (an external contact)")
    return {"role": role, "user": user} if user else {"role": role, "name": name, **({"email": email} if email else {})}


def _fields(**given: Any) -> dict[str, Any]:
    """The options the user actually passed (``None`` and empty tuples dropped); refuses an update that changes nothing."""
    out = {k: (list(v) if isinstance(v, tuple) else v) for k, v in given.items() if v is not None and v != ()}
    if not out:
        raise click.UsageError("nothing to change: pass at least one option (see --help)")
    return out


def _ok(state: State, text: str) -> Callable[[Any], None]:
    return lambda _d: state.out.success(text)


# --------------------------------------------------------------------------------------------- group, overview, asset
@click.group("governance", help="Business glossary, domains, data products, owners, classifications, review queue, certification, change requests, policies and quality.",
             epilog="\b\nExamples:\n"
                    "  olcf governance summary                                  coverage, review queue, violations\n"
                    "  olcf governance asset lakehouse_a.clean                  everything governance knows about a table\n"
                    "  olcf governance terms search revenue                     find glossary terms (names, synonyms, abbreviations)\n"
                    "  olcf governance exposure PII                             which reports and tables does PII data reach?\n"
                    "  olcf governance review-queue list                        pending classification and glossary suggestions\n"
                    "  olcf governance certify lakehouse_a.mart --comment ok   certify an asset (needs governance.approve)\n"
                    "  olcf governance policies evaluate                        evaluate every enabled policy now\n"
                    "  olcf governance violations list --severity high          open policy violations" + HELP_FOOTER)
def governance_group() -> None:
    pass


@governance_group.command("summary", help="Overview: glossary, domains, products, classifications, review queue, violations, certification and coverage.",
                          epilog="\b\nExamples:\n  olcf governance summary\n  olcf governance summary --workspace-id wks_... --json")
@click.option("--workspace-id", default=None, help="Only the assets of this workspace.")
@json_option
@pass_state
def summary_cmd(state: State, workspace_id: str | None) -> None:
    def human(d: dict[str, Any]) -> None:
        state.out.kv(G.summary_pairs(d))
        for title, key in (("least owned datasets", "top_unowned"), ("datasets without a reviewed classification", "top_unclassified")):
            if d.get(key):
                state.out.text("")
                state.out.text(f"{title}: " + ", ".join(G.asset_text(a) for a in d[key]))

    state.out.emit(state.client.governance.summary(workspace_id=workspace_id), human)


@governance_group.command("asset", help="Everything governance knows about one asset: domain, products, terms, owners, labels, certification, quality, violations and open requests.",
                          epilog="\b\nExamples:\n  olcf governance asset lakehouse_a.clean\n  olcf governance asset sales_report --type report --json")
@click.argument("asset")
@click.option("--type", "asset_type", default=None, help="Only consider assets of this type when resolving ASSET.")
@json_option
@pass_state
def asset_cmd(state: State, asset: str, asset_type: str | None) -> None:
    d = state.client.governance.asset(resolve_asset(state, asset, asset_type))

    def human(d: dict[str, Any]) -> None:
        o = state.out
        a, cls, cert = d.get("asset") or {}, d.get("classifications") or {}, d.get("certification") or {}
        dom = d.get("domain")
        o.kv([("asset", f"{a.get('name')} ({a.get('type')})  {a.get('path') or ''}".rstrip()), ("id", a.get("id")),
              ("domain", f"{dom['domain']['name']} ({dom.get('source')})" if dom else "none"),
              ("data products", ", ".join(p["name"] for p in d.get("products") or []) or "none"),
              ("glossary terms", ", ".join(t["name"] for t in d.get("terms") or []) or "none"),
              ("pending term links", ", ".join(t["name"] for t in d.get("pending_term_links") or []) or "none"),
              ("owners", G.owners_text((d.get("owners") or {}).get("effective"))),
              ("labels", f"direct {G.classes_text(cls.get('direct'))}; inherited {G.classes_text(cls.get('inherited'))}; suggested {G.classes_text(cls.get('suggestions'))}"
                         + (f"; contains {', '.join(c['classification']['name'] for c in cls['contained'])}" if cls.get("contained") else "")),
              ("certification", str(cert.get("state")) + (f" (invalidated: {cert['invalidated_reason']})" if cert.get("invalidated_reason") else "")),
              ("quality", f"{(d.get('quality') or {}).get('score')}/100  (olcf governance quality {a.get('name')})"),
              ("open requests", ", ".join(f"{c['id']} {c['type']}" for c in d.get("open_changes") or []) or "none")])
        if d.get("violations"):
            o.text("")
            o.text("Violations:")
            o.table(G.violation_rows(d["violations"]), [("ID", "id", 30), ("SEVERITY", "severity"), ("STATUS", "status"), ("POLICY", "policy", 30), ("MESSAGE", "message", 70)])

    state.out.emit(d, human)


@governance_group.command("graph", help="The virtual governance edges of an asset (governed by a domain / product / term, classified as a label); never stored in the lineage graph.",
                          epilog="\b\nExample:\n  olcf governance graph lakehouse_a.mart")
@click.argument("asset")
@json_option
@pass_state
def graph_cmd(state: State, asset: str) -> None:
    doc = state.client.governance.asset_graph(resolve_asset(state, asset))
    names = {n["id"]: f"{n['name']} ({n['type']})" for n in doc.get("nodes") or []}

    def human(d: dict[str, Any]) -> None:
        rows = [{"from": names.get(e["from"], e["from"]), "type": e["type"], "to": names.get(e["to"], e["to"]), "via": (e.get("attrs") or {}).get("via") or (e.get("attrs") or {}).get("source") or ""}
                for e in d.get("edges") or []]
        state.out.table(rows, [("FROM", "from", 30), ("EDGE", "type"), ("TO", "to", 40), ("VIA", "via")], empty="no governance edges")

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- glossary
@click.group("terms", help="The business glossary: terms with definitions, synonyms and abbreviations, linked to assets and data products.",
             epilog="\b\nExamples:\n  olcf governance terms search revenue\n  olcf governance terms create 'Net revenue' --definition 'Revenue after returns' --abbreviation NR\n"
                    "  olcf governance terms link 'Net revenue' lakehouse_a.mart")
def terms_group() -> None:
    pass


TERM_COLS = [("ID", "id", 30), ("NAME", "name", 30), ("ABBR", "abbreviation", 10), ("STATUS", "status"), ("CERTIFICATION", "certification"), ("LINKS", "links"), ("DEFINITION", "definition", 60)]


@terms_group.command("list", help="List glossary terms in creation order.", epilog="\b\nExamples:\n  olcf governance terms list --status approved\n  olcf governance terms list --parent root --all")
@click.option("-q", "--query", "q", default=None, help="Substring of name, synonym or definition.")
@click.option("--status", type=click.Choice(TERM_STATUSES), default=None)
@click.option("--glossary", default=None, help="Glossary name (default: default).")
@click.option("--domain", "domain", default=None, help="Domain name or id.")
@click.option("--parent", "parent", default=None, help="Parent term name or id; 'root' lists the top level.")
@click.option("--certification", type=click.Choice(CERT_STATES), default=None)
@click.option("--owner", default=None)
@paging_options
@json_option
@pass_state
def terms_list(state: State, q: str | None, status: str | None, glossary: str | None, domain: str | None, parent: str | None, certification: str | None, owner: str | None,
               limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_terms, limit=limit, fetch_all=fetch_all, q=q, status=status, glossary=glossary,
                     domain_id=resolve_named(state, "domain", domain) if domain else None,
                     parent_id=("root" if parent == "root" else resolve_named(state, "term", parent)) if parent else None, certification=certification, owner=owner)
    state.out.table(G.term_rows(rows) if state.out.fmt not in ("json", "yaml") else rows, TERM_COLS, empty="no glossary terms yet (create one: `olcf governance terms create NAME`)")


@terms_group.command("search", help="Ranked search over names, synonyms, abbreviations and definitions (exact name 100, synonym 90, abbreviation 80, prefix 60 ...).",
                     epilog="\b\nExamples:\n  olcf governance terms search revenue\n  olcf governance terms search NR --include-deprecated")
@click.argument("text")
@click.option("--limit", type=click.IntRange(1, 100), default=20, show_default=True)
@click.option("--include-deprecated", is_flag=True, help="Also search deprecated terms.")
@json_option
@pass_state
def terms_search(state: State, text: str, limit: int, include_deprecated: bool) -> None:
    hits = state.client.governance.search_glossary(text, limit=limit, include_deprecated=include_deprecated)
    state.out.table(G.search_rows(hits) if state.out.fmt not in ("json", "yaml") else list(hits),
                    [("ID", "id", 30), ("TERM", "name", 30), ("STATUS", "status"), ("SCORE", "score"), ("MATCHED ON", "matched"), ("DEFINITION", "definition", 60)], empty=f"no term matches '{text}'")


@terms_group.command("show", help="One term: definition, synonyms, hierarchy, related terms, owners, certification and link count.",
                     epilog="\b\nExamples:\n  olcf governance terms show 'Net revenue'\n  olcf governance terms show trm_01... --json")
@click.argument("term")
@json_option
@pass_state
def terms_show(state: State, term: str) -> None:
    doc = state.client.governance.get_term(resolve_named(state, "term", term))

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.kv([("id", d.get("id")), ("name", d.get("name")), ("glossary", d.get("glossary")), ("status", d.get("status")), ("certification", d.get("certification")),
              ("abbreviation", d.get("abbreviation")), ("synonyms", ", ".join(d.get("synonyms") or []) or None),
              ("hierarchy", " > ".join([a["name"] for a in d.get("ancestors") or []] + [d.get("name")]) if d.get("ancestors") else None),
              ("children", ", ".join(c["name"] for c in d.get("children") or []) or None), ("related", ", ".join(t["name"] for t in d.get("related_terms") or []) or None),
              ("owners", G.owners_text(d.get("owners"))), ("linked assets", d.get("link_count")), ("etag", doc.etag)])
        if d.get("definition"):
            o.text("")
            o.text(d["definition"])

    state.out.emit(doc, human)


@terms_group.command("create", help="Create a term (names are unique per glossary, case-insensitive). A new term is a draft; approving needs governance.approve.",
                     epilog="\b\nExamples:\n  olcf governance terms create 'Net revenue' --definition 'Revenue after returns' --abbreviation NR --synonym turnover\n"
                            "  olcf governance terms create 'Booked revenue' --parent 'Net revenue' --domain Finance")
@click.argument("name")
@click.option("--definition", default=None, help="Markdown text.")
@click.option("--abbreviation", default=None)
@click.option("--synonym", "synonyms", multiple=True, help="Another name for the same thing (repeatable).")
@click.option("--glossary", default=None)
@click.option("--status", type=click.Choice(TERM_STATUSES), default=None)
@click.option("--parent", default=None, help="Parent term (name or id) in the same glossary.")
@click.option("--domain", default=None, help="Domain (name or id).")
@click.option("--related", "related", multiple=True, help="A related term (name or id, repeatable).")
@click.option("--owner", "owners", multiple=True, metavar="USER", help="Owner: an OLCF user id, e-mail or user name (repeatable).")
@json_option
@pass_state
def terms_create(state: State, name: str, definition: str | None, abbreviation: str | None, synonyms: tuple[str, ...], glossary: str | None, status: str | None, parent: str | None,
                 domain: str | None, related: tuple[str, ...], owners: tuple[str, ...]) -> None:
    doc = state.client.governance.create_term(
        name, definition=definition, abbreviation=abbreviation, synonyms=list(synonyms) or None, glossary=glossary, status=status,
        parent_id=resolve_named(state, "term", parent) if parent else None, domain_id=resolve_named(state, "domain", domain) if domain else None,
        related=[resolve_named(state, "term", r) for r in related] or None, owners=[{"role": "owner", "user": u} for u in owners] or None)
    state.out.emit(doc, lambda d: state.out.success(f"created term {d['name']} ({d['id']}), status {d['status']}"))


@terms_group.command("update", help="Change a term. Approving (--status approved) needs governance.approve; a parent cycle is refused.",
                     epilog="\b\nExamples:\n  olcf governance terms update 'Net revenue' --definition 'Revenue after returns and credits'\n  olcf governance terms update NR --status approved")
@click.argument("term")
@click.option("--name", "new_name", default=None)
@click.option("--definition", default=None)
@click.option("--abbreviation", default=None)
@click.option("--synonym", "synonyms", multiple=True, help="Replace the synonyms (repeatable).")
@click.option("--status", type=click.Choice(TERM_STATUSES), default=None)
@click.option("--parent", default=None, help="New parent term (name or id).")
@click.option("--domain", default=None)
@if_match_option
@json_option
@pass_state
def terms_update(state: State, term: str, new_name: str | None, definition: str | None, abbreviation: str | None, synonyms: tuple[str, ...], status: str | None, parent: str | None,
                 domain: str | None, if_match: str | None) -> None:
    fields = _fields(name=new_name, definition=definition, abbreviation=abbreviation, synonyms=synonyms, status=status,
                     parent_id=resolve_named(state, "term", parent) if parent else None, domain_id=resolve_named(state, "domain", domain) if domain else None)
    doc = state.client.governance.update_term(resolve_named(state, "term", term), if_match, **fields)
    state.out.emit(doc, lambda d: state.out.success(f"updated term {d['name']} ({d['id']}); status {d['status']}"))


@terms_group.command("delete", help="Deprecate a term and unlink it everywhere (history is kept); with --hard remove it for good (needs governance.approve).",
                     epilog="\b\nExamples:\n  olcf governance terms delete 'Net revenue' --yes\n  olcf governance terms delete trm_01... --hard --yes")
@click.argument("term")
@click.option("--hard", is_flag=True, help="Remove the term, its links and references for good.")
@if_match_option
@yes_option
@json_option
@pass_state
def terms_delete(state: State, term: str, hard: bool, if_match: str | None, yes: bool) -> None:
    tid = resolve_named(state, "term", term)
    confirm(state, f"{'Permanently delete' if hard else 'Deprecate'} glossary term {term}?", yes)
    res = state.client.governance.delete_term(tid, hard=hard, if_match=if_match)
    state.out.emit(res, lambda d: state.out.success(("deleted" if d.get("deleted") else "deprecated") + f" term {term} ({d.get('links_unlinked', 0)} link(s) removed)"))


@terms_group.command("links", help="Assets and data products linked to a term.", epilog="\b\nExample:\n  olcf governance terms links 'Net revenue'")
@click.argument("term")
@click.option("--status", default=None, help="approved, pending, rejected or unlinked (default: approved and pending).")
@paging_options
@json_option
@pass_state
def terms_links(state: State, term: str, status: str | None, limit: int, fetch_all: bool) -> None:
    rows = _term_links(state, resolve_named(state, "term", term), status, limit, fetch_all)
    state.out.table(G.link_rows(rows) if state.out.fmt not in ("json", "yaml") else rows,
                    [("LINK", "id"), ("TERM", "term", 30), ("KIND", "kind"), ("TARGET", "target", 50), ("STATUS", "status"), ("SOURCE", "source"), ("CONFIDENCE", "confidence")], empty="no links")


def _term_links(state: State, term_id: str, status: str | None, limit: int, fetch_all: bool) -> list[dict[str, Any]]:
    g = state.client.governance
    if not fetch_all:
        return list(g.term_links(term_id, status=status, limit=limit))
    return list(g.iter_term_links(term_id, status=status, limit=limit))


@terms_group.command("link", help="Link a term to an asset (or, with --product, to a data product). Manual links are approved at once.",
                     epilog="\b\nExamples:\n  olcf governance terms link 'Net revenue' lakehouse_a.mart\n  olcf governance terms link 'Net revenue' 'Revenue mart' --product")
@click.argument("term")
@click.argument("target")
@click.option("--product", "is_product", is_flag=True, help="TARGET is a data product, not an asset.")
@click.option("--comment", default=None)
@click.option("--type", "asset_type", default=None, help="Only consider assets of this type when resolving TARGET.")
@json_option
@pass_state
def terms_link(state: State, term: str, target: str, is_product: bool, comment: str | None, asset_type: str | None) -> None:
    tid = resolve_named(state, "term", term)
    kw = {"product_id": resolve_named(state, "product", target)} if is_product else {"asset_id": resolve_asset(state, target, asset_type)}
    doc = state.client.governance.link_term(tid, comment=comment, **kw)
    state.out.emit(doc, lambda d: state.out.success(f"linked {d['term']['name']} to {G.asset_text(d.get('asset')) if d.get('asset') else (d.get('product') or {}).get('name')} (link {d['id']}, {d['status']})"))


@terms_group.command("unlink", help="Remove a term from an asset (the link is kept as history).", epilog="\b\nExample:\n  olcf governance terms unlink 'Net revenue' lakehouse_a.mart")
@click.argument("term")
@click.argument("asset")
@json_option
@pass_state
def terms_unlink(state: State, term: str, asset: str) -> None:
    res = state.client.governance.remove_asset_term(resolve_asset(state, asset), resolve_named(state, "term", term))
    state.out.emit(res, _ok(state, f"unlinked {term} from {asset}"))


@terms_group.command("suggest-links", help="Propose term links by exact name, synonym or abbreviation match; they wait in the review queue (nothing is approved).",
                     epilog="\b\nExample:\n  olcf governance terms suggest-links --workspace-id wks_...")
@click.option("--workspace-id", default=None)
@json_option
@pass_state
def terms_suggest(state: State, workspace_id: str | None) -> None:
    res = state.client.governance.suggest_term_links(workspace_id=workspace_id)
    state.out.emit(res, lambda d: state.out.success(f"examined {d.get('examined')} asset(s), {d.get('suggested')} new suggestion(s); review them with `olcf governance review-queue list`"))


# --------------------------------------------------------------------------------------------- domains
@click.group("domains", help="Business domains (a hierarchy) that own assets, terms and data products.",
             epilog="\b\nExamples:\n  olcf governance domains create Finance --description 'Money'\n  olcf governance domains assign lakehouse_a.mart Finance")
def domains_group() -> None:
    pass


DOMAIN_COLS = [("ID", "id", 30), ("NAME", "name", 30), ("ASSETS", "asset_count"), ("PRODUCTS", "product_count"), ("SUB-DOMAINS", "child_count"), ("DESCRIPTION", "description", 60)]


@domains_group.command("list", help="List domains with asset, product and sub-domain counts.", epilog="\b\nExamples:\n  olcf governance domains list\n  olcf governance domains list --parent Finance")
@click.option("-q", "--query", "q", default=None)
@click.option("--parent", default=None, help="Parent domain (name or id); 'root' lists the top level.")
@paging_options
@json_option
@pass_state
def domains_list(state: State, q: str | None, parent: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_domains, limit=limit, fetch_all=fetch_all, q=q, parent_id=("root" if parent == "root" else resolve_named(state, "domain", parent)) if parent else None)
    state.out.table(rows, DOMAIN_COLS, empty="no domains yet (create one: `olcf governance domains create NAME`)")


@domains_group.command("show", help="One domain with its path, sub-domains and owners.", epilog="\b\nExample:\n  olcf governance domains show Finance")
@click.argument("domain")
@json_option
@pass_state
def domains_show(state: State, domain: str) -> None:
    doc = state.client.governance.get_domain(resolve_named(state, "domain", domain))

    def human(d: dict[str, Any]) -> None:
        state.out.kv([("id", d.get("id")), ("name", d.get("name")), ("path", " > ".join([a["name"] for a in d.get("ancestors") or []] + [d.get("name")])),
                      ("description", d.get("description")), ("sub-domains", ", ".join(c["name"] for c in d.get("children") or []) or None), ("owners", G.owners_text(d.get("owners"))),
                      ("assets", d.get("asset_count")), ("data products", d.get("product_count")), ("etag", doc.etag)])

    state.out.emit(doc, human)


@domains_group.command("create", help="Create a domain.", epilog="\b\nExamples:\n  olcf governance domains create Finance\n  olcf governance domains create 'Sales analytics' --parent Finance --owner alice")
@click.argument("name")
@click.option("--description", default=None)
@click.option("--parent", default=None, help="Parent domain (name or id).")
@click.option("--owner", "owners", multiple=True, metavar="USER", help="Owner: an OLCF user id, e-mail or user name (repeatable).")
@json_option
@pass_state
def domains_create(state: State, name: str, description: str | None, parent: str | None, owners: tuple[str, ...]) -> None:
    doc = state.client.governance.create_domain(name, description=description, parent_id=resolve_named(state, "domain", parent) if parent else None,
                                                owners=[{"role": "owner", "user": u} for u in owners] or None)
    state.out.emit(doc, lambda d: state.out.success(f"created domain {d['name']} ({d['id']})"))


@domains_group.command("update", help="Rename or move a domain.", epilog="\b\nExample:\n  olcf governance domains update Finance --description 'Money and risk'")
@click.argument("domain")
@click.option("--name", "new_name", default=None)
@click.option("--description", default=None)
@click.option("--parent", default=None, help="New parent domain (name or id).")
@if_match_option
@json_option
@pass_state
def domains_update(state: State, domain: str, new_name: str | None, description: str | None, parent: str | None, if_match: str | None) -> None:
    fields = _fields(name=new_name, description=description, parent_id=resolve_named(state, "domain", parent) if parent else None)
    doc = state.client.governance.update_domain(resolve_named(state, "domain", domain), if_match, **fields)
    state.out.emit(doc, lambda d: state.out.success(f"updated domain {d['name']} ({d['id']})"))


@domains_group.command("delete", help="Delete a domain (refused while it has sub-domains, products, assigned assets or linked terms).",
                       epilog="\b\nExample:\n  olcf governance domains delete 'Old domain' --yes")
@click.argument("domain")
@if_match_option
@yes_option
@json_option
@pass_state
def domains_delete(state: State, domain: str, if_match: str | None, yes: bool) -> None:
    did = resolve_named(state, "domain", domain)
    confirm(state, f"Delete domain {domain}?", yes)
    state.out.emit(state.client.governance.delete_domain(did, if_match=if_match), _ok(state, f"deleted domain {domain}"))


@domains_group.command("assets", help="Assets in a domain: assigned directly, inherited from a parent asset or reached through a data product.",
                       epilog="\b\nExamples:\n  olcf governance domains assets Finance --type table\n  olcf governance domains assets Finance --no-descendants")
@click.argument("domain")
@click.option("--type", "type_", default=None, help="Asset type.")
@click.option("--no-descendants", is_flag=True, help="Leave out the assets of sub-domains.")
@paging_options
@json_option
@pass_state
def domains_assets(state: State, domain: str, type_: str | None, no_descendants: bool, limit: int, fetch_all: bool) -> None:
    did = resolve_named(state, "domain", domain)
    kw: dict[str, Any] = {"type": type_, "include_descendants": not no_descendants}
    rows = list(state.client.governance.iter_domain_assets(did, limit=limit, **kw)) if fetch_all else list(state.client.governance.domain_assets(did, limit=limit, **kw))
    state.out.table(rows, [("ID", "id", 30), ("TYPE", "type"), ("NAME", "name", 30), ("PATH", "path", 40), ("VIA", "domain_source")], empty="no assets in this domain")


@domains_group.command("assign", help="Put an asset in a domain (its columns and children inherit it); --clear removes the assignment.",
                       epilog="\b\nExamples:\n  olcf governance domains assign lakehouse_a.mart Finance\n  olcf governance domains assign lakehouse_a.mart --clear")
@click.argument("asset")
@click.argument("domain", required=False)
@click.option("--clear", is_flag=True, help="Remove the asset's domain.")
@click.option("--type", "asset_type", default=None)
@json_option
@pass_state
def domains_assign(state: State, asset: str, domain: str | None, clear: bool, asset_type: str | None) -> None:
    if bool(domain) == clear:
        raise click.UsageError("give a DOMAIN to assign, or --clear to remove the assignment")
    aid = resolve_asset(state, asset, asset_type)
    res = state.client.governance.set_asset_domain(aid, None if clear else resolve_named(state, "domain", domain or ""))
    state.out.emit(res, _ok(state, f"cleared the domain of {asset}" if clear else f"assigned {asset} to domain {domain}"))


# --------------------------------------------------------------------------------------------- data products
@click.group("products", help="Data products: a governed set of assets with an owner, a domain, an SLA and a contract, and the consumers derived from lineage.",
             epilog="\b\nExamples:\n  olcf governance products create 'Revenue mart' --domain Finance --member lakehouse_a.mart\n  olcf governance products consumers 'Revenue mart'")
def products_group() -> None:
    pass


PRODUCT_COLS = [("ID", "id", 30), ("NAME", "name", 30), ("STATUS", "status"), ("CERTIFICATION", "certification"), ("DOMAIN", "domain.name", 24), ("MEMBERS", "member_count")]


@products_group.command("list", help="List data products.", epilog="\b\nExamples:\n  olcf governance products list --status published\n  olcf governance products list --domain Finance")
@click.option("-q", "--query", "q", default=None)
@click.option("--status", type=click.Choice(PRODUCT_STATUSES), default=None)
@click.option("--domain", default=None, help="Domain (name or id).")
@click.option("--certification", type=click.Choice(CERT_STATES), default=None)
@click.option("--owner", default=None)
@paging_options
@json_option
@pass_state
def products_list(state: State, q: str | None, status: str | None, domain: str | None, certification: str | None, owner: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_products, limit=limit, fetch_all=fetch_all, q=q, status=status, domain_id=resolve_named(state, "domain", domain) if domain else None,
                     certification=certification, owner=owner)
    state.out.table(rows, PRODUCT_COLS, empty="no data products yet (create one: `olcf governance products create NAME`)")


@products_group.command("show", help="One data product: members, consumers, SLA, contract, owners, terms, certification and quality.", epilog="\b\nExample:\n  olcf governance products show 'Revenue mart'")
@click.argument("product")
@json_option
@pass_state
def products_show(state: State, product: str) -> None:
    doc = state.client.governance.get_product(resolve_named(state, "product", product))

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.kv([("id", d.get("id")), ("name", d.get("name")), ("status", d.get("status")), ("certification", d.get("certification")), ("domain", (d.get("domain") or {}).get("name")),
              ("description", d.get("description")), ("owners", G.owners_text(d.get("owners"))), ("sla", json.dumps(d["sla"], sort_keys=True) if d.get("sla") else None),
              ("contract", f"{len((d['contract'] or {}).get('columns') or [])} column(s), {len((d['contract'] or {}).get('expectations') or [])} expectation(s)" if d.get("contract") else None),
              ("terms", ", ".join(t["name"] for t in d.get("terms") or []) or None), ("quality", (d.get("quality") or {}).get("score")), ("etag", doc.etag)])
        o.text("")
        o.text(f"Members ({d.get('member_count')}" + (f", {d['hidden_members']} hidden from you" if d.get("hidden_members") else "") + "):")
        o.table(d.get("members") or [], [("ID", "id", 30), ("TYPE", "type"), ("NAME", "name", 30), ("PATH", "path", 40)], empty="no members")
        if d.get("consumers"):
            o.text("")
            o.text("Consumers (downstream, not members):")
            o.table(_consumer_rows(d["consumers"]), CONSUMER_COLS)

    state.out.emit(doc, human)


CONSUMER_COLS = [("CONSUMER", "consumer", 40), ("TYPE", "type"), ("READS", "via", 30), ("HOPS", "hops"), ("CONFIDENCE", "confidence")]


def _consumer_rows(items: Any) -> list[dict[str, Any]]:
    return [{"consumer": G.asset_text(c.get("asset")), "type": (c.get("asset") or {}).get("type"), "via": (c.get("via_member") or {}).get("name"), "hops": c.get("hops"),
             "confidence": c.get("confidence")} for c in items]


@products_group.command("create", help="Create a data product. A new product is a draft; publishing needs governance.approve.",
                        epilog="\b\nExamples:\n  olcf governance products create 'Revenue mart' --domain Finance --member lakehouse_a.mart --freshness-hours 24\n"
                               "  olcf governance products create Orders --owner alice --contract-file contract.json")
@click.argument("name")
@click.option("--description", default=None)
@click.option("--domain", default=None, help="Domain (name or id).")
@click.option("--status", type=click.Choice(PRODUCT_STATUSES), default=None)
@click.option("--member", "members", multiple=True, help="A member asset (repeatable): table, view, report, semantic model ...")
@click.option("--owner", "owners", multiple=True, metavar="USER", help="Owner: an OLCF user id, e-mail or user name (repeatable).")
@click.option("--freshness-hours", type=float, default=None, help="SLA: data is at most this old.")
@click.option("--availability", default=None, help="SLA: free text such as '99.5%'.")
@click.option("--contract-file", default=None, help="JSON file (or - for stdin) with {columns: [...], expectations: [...]}.")
@json_option
@pass_state
def products_create(state: State, name: str, description: str | None, domain: str | None, status: str | None, members: tuple[str, ...], owners: tuple[str, ...],
                    freshness_hours: float | None, availability: str | None, contract_file: str | None) -> None:
    sla = {k: v for k, v in (("freshness_hours", freshness_hours), ("availability", availability)) if v is not None}
    contract = _load_json_object(None, contract_file, (), "contract") if contract_file else None
    doc = state.client.governance.create_product(
        name, description=description, domain_id=resolve_named(state, "domain", domain) if domain else None, status=status,
        owners=[{"role": "owner", "user": u} for u in owners] or None, member_asset_ids=[resolve_asset(state, m) for m in members] or None, sla=sla or None, contract=contract)
    state.out.emit(doc, lambda d: state.out.success(f"created data product {d['name']} ({d['id']}), status {d['status']}, {d['member_count']} member(s)"))


@products_group.command("update", help="Change a data product. Publishing (--status published) needs governance.approve.",
                        epilog="\b\nExamples:\n  olcf governance products update 'Revenue mart' --status published\n  olcf governance products update 'Revenue mart' --freshness-hours 12")
@click.argument("product")
@click.option("--name", "new_name", default=None)
@click.option("--description", default=None)
@click.option("--domain", default=None, help="Domain (name or id).")
@click.option("--status", type=click.Choice(PRODUCT_STATUSES), default=None)
@click.option("--freshness-hours", type=float, default=None)
@click.option("--availability", default=None)
@if_match_option
@json_option
@pass_state
def products_update(state: State, product: str, new_name: str | None, description: str | None, domain: str | None, status: str | None, freshness_hours: float | None,
                    availability: str | None, if_match: str | None) -> None:
    sla = {k: v for k, v in (("freshness_hours", freshness_hours), ("availability", availability)) if v is not None}
    fields = _fields(name=new_name, description=description, domain_id=resolve_named(state, "domain", domain) if domain else None, status=status, sla=sla or None)
    doc = state.client.governance.update_product(resolve_named(state, "product", product), if_match, **fields)
    state.out.emit(doc, lambda d: state.out.success(f"updated data product {d['name']} ({d['id']}); status {d['status']}"))


@products_group.command("delete", help="Delete a data product (refused while it is published: deprecate it first).", epilog="\b\nExample:\n  olcf governance products delete 'Old product' --yes")
@click.argument("product")
@if_match_option
@yes_option
@json_option
@pass_state
def products_delete(state: State, product: str, if_match: str | None, yes: bool) -> None:
    pid = resolve_named(state, "product", product)
    confirm(state, f"Delete data product {product}?", yes)
    state.out.emit(state.client.governance.delete_product(pid, if_match=if_match), _ok(state, f"deleted data product {product}"))


@products_group.command("add-member", help="Add an asset to a data product.", epilog="\b\nExample:\n  olcf governance products add-member 'Revenue mart' lakehouse_a.clean")
@click.argument("product")
@click.argument("asset")
@json_option
@pass_state
def products_add_member(state: State, product: str, asset: str) -> None:
    res = state.client.governance.add_product_member(resolve_named(state, "product", product), resolve_asset(state, asset))
    state.out.emit(res, _ok(state, f"added {asset} to {product}"))


@products_group.command("remove-member", help="Remove an asset from a data product.", epilog="\b\nExample:\n  olcf governance products remove-member 'Revenue mart' lakehouse_a.clean")
@click.argument("product")
@click.argument("asset")
@json_option
@pass_state
def products_remove_member(state: State, product: str, asset: str) -> None:
    res = state.client.governance.remove_product_member(resolve_named(state, "product", product), resolve_asset(state, asset))
    state.out.emit(res, _ok(state, f"removed {asset} from {product}"))


@products_group.command("set-members", help="Replace the member set of a data product.", epilog="\b\nExample:\n  olcf governance products set-members 'Revenue mart' lakehouse_a.mart lakehouse_a.clean")
@click.argument("product")
@click.argument("assets", nargs=-1, required=True)
@json_option
@pass_state
def products_set_members(state: State, product: str, assets: tuple[str, ...]) -> None:
    res = state.client.governance.set_product_members(resolve_named(state, "product", product), [resolve_asset(state, a) for a in assets])
    state.out.emit(res, _ok(state, f"{product} now has {len(assets)} member(s)"))


@products_group.command("consumers", help="Who reads a data product: assets and reports downstream of its members that are not members, with the path confidence.",
                        epilog="\b\nExamples:\n  olcf governance products consumers 'Revenue mart'\n  olcf governance products consumers 'Revenue mart' --depth 2 --min-confidence derived")
@click.argument("product")
@click.option("--depth", type=click.IntRange(1, 6), default=None, help="Lineage hops to follow (default 3).")
@click.option("--min-confidence", type=click.Choice(CONFIDENCES), default=None)
@click.option("--limit", type=click.IntRange(1, 500), default=200, show_default=True)
@json_option
@pass_state
def products_consumers(state: State, product: str, depth: int | None, min_confidence: str | None, limit: int) -> None:
    doc = state.client.governance.product_consumers(resolve_named(state, "product", product), depth=depth, min_confidence=min_confidence, limit=limit)

    def human(d: dict[str, Any]) -> None:
        state.out.table(_consumer_rows(d.get("items") or []), CONSUMER_COLS, empty="no consumers found in lineage")
        if d.get("truncated"):
            state.out.text(f"(showing {len(d['items'])} of {d.get('total')}; raise --limit)")

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- owners
@click.group("owners", help="Owners, stewards, custodians and contacts of assets, domains, data products and glossary terms (assets inherit from their parents).",
             epilog="\b\nExamples:\n  olcf governance owners list asset lakehouse_a.clean\n  olcf governance owners add asset lakehouse_a.clean --role owner --user alice\n"
                    "  olcf governance owners add domain Finance --role steward --name 'Data Office' --email data-office@example.com")
def owners_group() -> None:
    pass


@owners_group.command("list", help="Direct and effective (inherited) owners of a subject.", epilog="\b\nExamples:\n  olcf governance owners list asset lakehouse_a.clean\n  olcf governance owners list domain Finance")
@click.argument("kind", type=click.Choice(SUBJECT_KINDS))
@click.argument("subject")
@json_option
@pass_state
def owners_list(state: State, kind: str, subject: str) -> None:
    doc = state.client.governance.owners(kind, resolve_subject(state, kind, subject))  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        rows = [{"scope": "direct", **o} for o in d.get("direct") or []] + [{"scope": "inherited", **o} for o in d.get("effective") or [] if o.get("inherited_from")]
        for r in rows:
            r["from"] = (r.get("inherited_from") or {}).get("name") or ""
        state.out.table(rows, [("ID", "id"), ("ROLE", "role"), ("NAME", "name", 30), ("EMAIL", "email", 30), ("KIND", "kind"), ("SCOPE", "scope"), ("FROM", "from", 24)], empty="no owners")

    state.out.emit(doc, human)


@owners_group.command("add", help="Add one owner: an OLCF user (--user) or an external contact (--name, --email). An existing assignment is refused (exit 6).",
                      epilog="\b\nExamples:\n  olcf governance owners add asset lakehouse_a.clean --role owner --user alice\n  olcf governance owners add product 'Revenue mart' --role steward --name 'Data Office'")
@click.argument("kind", type=click.Choice(SUBJECT_KINDS))
@click.argument("subject")
@click.option("--role", type=click.Choice(OWNER_ROLES), default="owner", show_default=True)
@click.option("--user", default=None, help="An OLCF user: id, e-mail or user name.")
@click.option("--name", default=None, help="An external contact's name.")
@click.option("--email", default=None, help="An external contact's e-mail.")
@json_option
@pass_state
def owners_add(state: State, kind: str, subject: str, role: str, user: str | None, name: str | None, email: str | None) -> None:
    body = _owner_body(role, user, name, email)
    res = state.client.governance.add_owner(kind, resolve_subject(state, kind, subject), body)  # type: ignore[arg-type]
    state.out.emit(res, lambda d: state.out.success(f"added {d.get('name')} as {d.get('role')} of {subject} (owner id {d.get('id')})"))


@owners_group.command("remove", help="Remove one direct owner by its owner id (see `owners list`).", epilog="\b\nExample:\n  olcf governance owners remove asset lakehouse_a.clean 7")
@click.argument("kind", type=click.Choice(SUBJECT_KINDS))
@click.argument("subject")
@click.argument("owner_id", type=int)
@json_option
@pass_state
def owners_remove(state: State, kind: str, subject: str, owner_id: int) -> None:
    res = state.client.governance.remove_owner(kind, resolve_subject(state, kind, subject), owner_id)  # type: ignore[arg-type]
    state.out.emit(res, _ok(state, f"removed owner {owner_id} from {subject}"))


@owners_group.command("set", help="Replace the direct owners from a JSON file (a list of {role, user} / {role, name, email}); an empty list clears them.",
                      epilog="\b\nExample:\n  echo '[{\"role\": \"owner\", \"user\": \"alice\"}]' | olcf governance owners set asset lakehouse_a.clean -")
@click.argument("kind", type=click.Choice(SUBJECT_KINDS))
@click.argument("subject")
@click.argument("file", default="-")
@json_option
@pass_state
def owners_set(state: State, kind: str, subject: str, file: str) -> None:
    try:
        owners = json.loads(read_input(file))
    except ValueError as exc:
        raise click.BadParameter(f"{file} is not valid JSON: {exc}") from None
    if not isinstance(owners, list):
        raise click.BadParameter("the file must hold a JSON list of owners")
    res = state.client.governance.set_owners(kind, resolve_subject(state, kind, subject), owners)  # type: ignore[arg-type]
    state.out.emit(res, _ok(state, f"{subject} now has {len(owners)} direct owner(s)"))


# --------------------------------------------------------------------------------------------- classifications
@click.group("classifications", help="Sensitivity labels (PII, PCI, Financial, Credentials, Restricted, your own): assign them, see where the labelled data flows, review suggestions.",
             epilog="\b\nExamples:\n  olcf governance classifications list\n  olcf governance classifications assign lakehouse_a.raw.email PII\n  olcf governance classifications exposure PII")
def classifications_group() -> None:
    pass


CLASS_COLS = [("ID", "id", 30), ("NAME", "name", 20), ("CATEGORY", "category"), ("LEVEL", "level"), ("BUILT-IN", "builtin"), ("ASSIGNED", "assignment_count"), ("DESCRIPTION", "description", 60)]


@classifications_group.command("list", help="The classification catalog (built-in and custom labels).", epilog="\b\nExample:\n  olcf governance classifications list")
@json_option
@pass_state
def classifications_list(state: State) -> None:
    state.out.table(list(state.client.governance.list_classifications()), CLASS_COLS, empty="no classifications")


@classifications_group.command("show", help="One label.", epilog="\b\nExample:\n  olcf governance classifications show PII")
@click.argument("label")
@json_option
@pass_state
def classifications_show(state: State, label: str) -> None:
    doc = state.client.governance.get_classification(resolve_named(state, "classification", label))
    state.out.emit(doc, lambda d: state.out.kv([("id", d.get("id")), ("name", d.get("name")), ("category", d.get("category")), ("level", d.get("level")), ("color", d.get("color")),
                                                ("built-in", d.get("builtin")), ("assigned", d.get("assignment_count")), ("description", d.get("description")), ("etag", doc.etag)]))


@classifications_group.command("create", help="Create a custom label (level 0..5: higher is more sensitive).",
                               epilog="\b\nExample:\n  olcf governance classifications create Embargoed --category handling --level 4 --color '#112233'")
@click.argument("name")
@click.option("--category", required=True, help="Grouping such as sensitivity, pii or handling.")
@click.option("--level", type=click.IntRange(0, 5), required=True)
@click.option("--description", default=None)
@click.option("--color", default=None, help="#rrggbb")
@json_option
@pass_state
def classifications_create(state: State, name: str, category: str, level: int, description: str | None, color: str | None) -> None:
    doc = state.client.governance.create_classification(name, category, level, description=description, color=color)
    state.out.emit(doc, lambda d: state.out.success(f"created label {d['name']} ({d['id']}), level {d['level']}"))


@classifications_group.command("update", help="Change a label (built-in labels allow only --description and --color).", epilog="\b\nExample:\n  olcf governance classifications update Embargoed --level 5")
@click.argument("label")
@click.option("--name", "new_name", default=None)
@click.option("--category", default=None)
@click.option("--level", type=click.IntRange(0, 5), default=None)
@click.option("--description", default=None)
@click.option("--color", default=None)
@if_match_option
@json_option
@pass_state
def classifications_update(state: State, label: str, new_name: str | None, category: str | None, level: int | None, description: str | None, color: str | None,
                           if_match: str | None) -> None:
    fields = _fields(name=new_name, category=category, level=level, description=description, color=color)
    doc = state.client.governance.update_classification(resolve_named(state, "classification", label), if_match, **fields)
    state.out.emit(doc, lambda d: state.out.success(f"updated label {d['name']} ({d['id']})"))


@classifications_group.command("delete", help="Delete a custom label (needs governance.approve; refused while it is assigned and for built-in labels).",
                               epilog="\b\nExample:\n  olcf governance classifications delete Embargoed --yes")
@click.argument("label")
@if_match_option
@yes_option
@json_option
@pass_state
def classifications_delete(state: State, label: str, if_match: str | None, yes: bool) -> None:
    cid = resolve_named(state, "classification", label)
    confirm(state, f"Delete label {label}?", yes)
    state.out.emit(state.client.governance.delete_classification(cid, if_match=if_match), _ok(state, f"deleted label {label}"))


@classifications_group.command("of", help="The labels of an asset: direct, inherited along lineage (with the evidence), suggested, rejected and contained in its columns.",
                               epilog="\b\nExamples:\n  olcf governance classifications of lakehouse_a.clean\n  olcf governance classifications of clean.amount --json")
@click.argument("asset")
@click.option("--type", "asset_type", default=None)
@json_option
@pass_state
def classifications_of(state: State, asset: str, asset_type: str | None) -> None:
    doc = state.client.governance.asset_classifications(resolve_asset(state, asset, asset_type))

    def human(d: dict[str, Any]) -> None:
        labels = column_labels(state, d)
        rows = []
        for bucket in ("direct", "inherited", "suggestions", "rejected"):
            for a in d.get(bucket) or []:
                path = ((a.get("evidence") or {}).get("path") or {}).get("nodes") or []
                rows.append({"label": (a.get("classification") or {}).get("name"), "how": bucket, "status": a.get("status"), "confidence": a.get("confidence"), "precision": a.get("precision") or "",
                             "evidence": " -> ".join(G.HIDDEN if n.get("hidden") else labels.get(str(n.get("id")), str(n.get("name"))) for n in path) or (a.get("comment") or "")})
        for c in d.get("contained") or []:
            rows.append({"label": c["classification"]["name"], "how": "contained", "status": "", "confidence": None, "precision": "", "evidence": f"{c['columns']} column(s)"})
        state.out.table(rows, [("LABEL", "label"), ("HOW", "how"), ("STATUS", "status"), ("CONFIDENCE", "confidence"), ("PRECISION", "precision"), ("EVIDENCE", "evidence", 70)], empty="no labels")

    state.out.emit(doc, human)


@classifications_group.command("assign", help="Label an asset. With four-eyes approval on, a curator's label waits in the review queue as a suggestion.",
                               epilog="\b\nExamples:\n  olcf governance classifications assign lakehouse_a.raw.email PII\n  olcf governance classifications assign lakehouse_a.clean Internal --comment 'by policy'")
@click.argument("asset")
@click.argument("label")
@click.option("--comment", default=None)
@click.option("--type", "asset_type", default=None)
@json_option
@pass_state
def classifications_assign(state: State, asset: str, label: str, comment: str | None, asset_type: str | None) -> None:
    doc = state.client.governance.classify_asset(resolve_asset(state, asset, asset_type), label, comment=comment)
    state.out.emit(doc, lambda d: state.out.success(f"labelled {asset} as {(d.get('classification') or {}).get('name')} ({d.get('status')})"))


@classifications_group.command("remove", help="Remove a label from an asset.", epilog="\b\nExample:\n  olcf governance classifications remove lakehouse_a.raw.email PII")
@click.argument("asset")
@click.argument("label")
@click.option("--type", "asset_type", default=None)
@json_option
@pass_state
def classifications_remove(state: State, asset: str, label: str, asset_type: str | None) -> None:
    res = state.client.governance.unclassify_asset(resolve_asset(state, asset, asset_type), label)
    state.out.emit(res, _ok(state, f"removed {label} from {asset}"))


@classifications_group.command("assets", help="Assets that carry a label, directly or (default) by inheritance along lineage. 'Which reports show PII?' starts at `exposure`.",
                               epilog="\b\nExamples:\n  olcf governance classifications assets PII\n  olcf governance classifications assets PII --direct --type table")
@click.argument("label")
@click.option("--direct", is_flag=True, help="Only assets labelled directly, not by inheritance.")
@click.option("--type", "type_", default=None)
@click.option("--workspace-id", default=None)
@paging_options
@json_option
@pass_state
def classifications_assets(state: State, label: str, direct: bool, type_: str | None, workspace_id: str | None, limit: int, fetch_all: bool) -> None:
    cid = resolve_named(state, "classification", label)
    kw: dict[str, Any] = {"workspace_id": workspace_id, "include_inherited": not direct, "type": type_}
    g = state.client.governance
    rows = list(g.iter_classification_assets(cid, limit=limit, **kw)) if fetch_all else list(g.classification_assets(cid, limit=limit, **kw))
    labels = column_labels(state, rows)
    flat = [{**r, "name": G.asset_text(r.get("asset"), labels), "type": (r.get("asset") or {}).get("type")} for r in rows]
    state.out.table(flat if state.out.fmt not in ("json", "yaml") else rows,
                    [("ASSET", "name", 50), ("TYPE", "type"), ("HOW", "source"), ("STATUS", "status"), ("CONFIDENCE", "confidence"), ("PRECISION", "precision")], empty="no assets carry this label")


def run_exposure(state: State, label: str, depth: int | None, min_confidence: str | None, proven_only: bool, types: tuple[str, ...], workspace_id: str | None, show_paths: bool) -> None:
    doc = state.client.governance.classification_exposure(resolve_named(state, "classification", label), depth=depth, min_confidence=min_confidence,
                                                          include_possible=not proven_only, types=list(types) or None, workspace_id=workspace_id)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        labels = column_labels(state, d)
        o.text(G.exposure_headline(d))
        srcs = ", ".join(G.asset_text(s.get("asset"), labels) for s in d.get("sources") or [])
        o.text(f"Labelled directly: {srcs or 'nothing'}")
        o.text("")
        o.table(G.exposure_rows(d, labels), [("ASSET", "asset", 44), ("TYPE", "type"), ("PRECISION", "precision"), ("PROVEN", "proven"), ("CONFIDENCE", "confidence"), ("FROM", "from", 30), ("HOPS", "hops")],
                empty="the labelled data does not reach any other asset")
        if show_paths and d.get("exposures"):
            o.text("")
            o.text("Lineage paths:")
            for line in G.exposure_paths(d, labels):
                o.text("  " + line)
        for note in d.get("notes") or []:
            o.text(f"note: {note}")
        if d.get("truncated"):
            o.text("(the lineage walk was cut at a configured limit; the list may be incomplete)")

    state.out.emit(doc, human)


def _exposure_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--paths", "show_paths", is_flag=True, help="Print the lineage path to every exposed asset.")(f)
    f = click.option("--workspace-id", default=None)(f)
    f = click.option("--type", "types", multiple=True, help="Only these asset types (repeatable): report, semantic_model, table, visual ...")(f)
    f = click.option("--proven-only", is_flag=True, help="Leave out possible exposures (table-level lineage only).")(f)
    f = click.option("--min-confidence", type=click.Choice(CONFIDENCES), default=None, help="Weakest lineage edge to follow (default: the governance.propagation_min_confidence setting).")(f)
    return click.option("--depth", type=click.IntRange(1, 10), default=None, help="Lineage hops (default: the governance.propagation_depth setting).")(f)


@classifications_group.command("exposure", help="Where does data labelled X end up? Reports, semantic models, tables and data products reached by lineage, with proven / possible and the path.",
                               epilog="\b\nExamples:\n  olcf governance classifications exposure PII\n  olcf governance classifications exposure PII --type report --min-confidence inferred --paths")
@click.argument("label")
@_exposure_options
@json_option
@pass_state
def classifications_exposure(state: State, label: str, depth: int | None, min_confidence: str | None, proven_only: bool, types: tuple[str, ...], workspace_id: str | None,
                             show_paths: bool) -> None:
    run_exposure(state, label, depth, min_confidence, proven_only, types, workspace_id, show_paths)


@governance_group.command("exposure", help="Which reports and tables does data labelled X reach? (same as `classifications exposure`).",
                          epilog="\b\nExamples:\n  olcf governance exposure PII\n  olcf governance exposure PII --type report --min-confidence inferred --paths")
@click.argument("label")
@_exposure_options
@json_option
@pass_state
def exposure_cmd(state: State, label: str, depth: int | None, min_confidence: str | None, proven_only: bool, types: tuple[str, ...], workspace_id: str | None, show_paths: bool) -> None:
    run_exposure(state, label, depth, min_confidence, proven_only, types, workspace_id, show_paths)


@classifications_group.command("scan", help="Ingest the sensitive-data suggestions found by discovery into the review queue (nothing is approved).",
                               epilog="\b\nExample:\n  olcf governance classifications scan")
@click.option("--workspace-id", default=None)
@click.option("--scan-id", default=None, help="Only the suggestions of this discovery scan.")
@json_option
@pass_state
def classifications_scan(state: State, workspace_id: str | None, scan_id: str | None) -> None:
    res = state.client.governance.scan_classifications(workspace_id=workspace_id, scan_id=scan_id)

    def human(d: dict[str, Any]) -> None:
        c, t = d.get("classification") or {}, d.get("term_links") or {}
        state.out.success(f"examined {d.get('assets_examined')} asset(s): {c.get('created', 0)} new label suggestion(s), {c.get('updated', 0)} refreshed, {c.get('withdrawn', 0)} withdrawn; "
                          f"{t.get('suggested', 0)} new term-link suggestion(s)")

    state.out.emit(res, human)


# --------------------------------------------------------------------------------------------- review queue
@click.group("review-queue", help="Pending classification and glossary-link suggestions. Suggestions are never approved automatically.",
             epilog="\b\nExamples:\n  olcf governance review-queue list --kind classification\n  olcf governance review-queue approve term_link:3\n  olcf governance review-queue reject classification:7 --comment 'test data'")
def review_group() -> None:
    pass


REVIEW_COLS = [("ID", "id", 24), ("KIND", "kind"), ("ASSET", "asset", 44), ("PROPOSAL", "proposal", 24), ("CONFIDENCE", "confidence"), ("SOURCE", "source"), ("NEEDS", "requires")]


@review_group.command("list", help="Pending suggestions, oldest first.", epilog="\b\nExamples:\n  olcf governance review-queue list\n  olcf governance review-queue list --classification PII --min-confidence 0.8")
@click.option("--kind", type=click.Choice(["classification", "term_link"]), default=None)
@click.option("--asset", default=None, help="Only suggestions for this asset.")
@click.option("--workspace-id", default=None)
@click.option("--classification", default=None, help="Only this label.")
@click.option("--term", default=None, help="Only this glossary term (name or id).")
@click.option("--min-confidence", type=click.FloatRange(0, 1), default=None)
@click.option("-q", "--query", "q", default=None)
@paging_options
@json_option
@pass_state
def review_list(state: State, kind: str | None, asset: str | None, workspace_id: str | None, classification: str | None, term: str | None, min_confidence: float | None, q: str | None,
                limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.review_queue, limit=limit, fetch_all=fetch_all, kind=kind, asset_id=resolve_asset(state, asset) if asset else None, workspace_id=workspace_id,
                     classification=classification, term_id=resolve_named(state, "term", term) if term else None, min_confidence=min_confidence, q=q)
    state.out.table(G.review_rows(rows) if state.out.fmt not in ("json", "yaml") else rows, [*REVIEW_COLS, ("CREATED", "created_at")], empty="the review queue is empty")


@review_group.command("summary", help="Counts of pending items by kind and open change requests.", epilog="\b\nExample:\n  olcf governance review-queue summary")
@click.option("--workspace-id", default=None)
@json_option
@pass_state
def review_summary(state: State, workspace_id: str | None) -> None:
    doc = state.client.governance.review_summary(workspace_id=workspace_id)
    state.out.emit(doc, lambda d: state.out.kv([("pending", d.get("total")), ("classification suggestions", d.get("classification")), ("term-link suggestions", d.get("term_link")),
                                                ("change requests", d.get("change_requests"))]))


def _decide(state: State, action: str, items: tuple[str, ...], comment: str | None) -> None:
    g = state.client.governance
    past = "approved" if action == "approve" else "rejected"
    if len(items) == 1:
        res = g.approve_review(items[0], comment=comment) if action == "approve" else g.reject_review(items[0], comment=comment)
        state.out.emit(res, lambda d: state.out.success(f"{past} {d.get('id')}" + (f" ({d['term']['name']})" if d.get("term") else "")))
        return
    res = g.review_bulk(action, list(items), comment=comment)  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        state.out.table([{"id": r["id"], "result": "ok" if r.get("ok") else "failed", "error": (r.get("error") or {}).get("message", "")} for r in d.get("results") or []],
                        [("ITEM", "id", 24), ("RESULT", "result"), ("REASON", "error", 80)])
        state.out.text(f"{past}: {d.get('ok')}, failed: {d.get('failed')}")

    state.out.emit(res, human)
    if res.get("failed"):
        raise SystemExit(EXIT_FAIL)


@review_group.command("approve", help="Approve one or more suggestions (ids such as term_link:3, classification:7). Several ids are decided one by one; a failure exits 1.",
                      epilog="\b\nExamples:\n  olcf governance review-queue approve term_link:3\n  olcf governance review-queue approve classification:7 classification:8 --comment reviewed")
@click.argument("items", nargs=-1, required=True)
@click.option("--comment", default=None)
@json_option
@pass_state
def review_approve(state: State, items: tuple[str, ...], comment: str | None) -> None:
    _decide(state, "approve", items, comment)


@review_group.command("reject", help="Reject one or more suggestions; a rejected suggestion is not proposed again.",
                      epilog="\b\nExamples:\n  olcf governance review-queue reject classification:7 --comment 'test data'\n  olcf governance review-queue reject term_link:3 term_link:4")
@click.argument("items", nargs=-1, required=True)
@click.option("--comment", default=None)
@json_option
@pass_state
def review_reject(state: State, items: tuple[str, ...], comment: str | None) -> None:
    _decide(state, "reject", items, comment)


# --------------------------------------------------------------------------------------------- certification
@governance_group.command("certify", help="Set the certification state of an asset, data product or term: certified / deprecated need governance.approve, draft / none need governance.curate.",
                          epilog="\b\nExamples:\n  olcf governance certify lakehouse_a.mart --comment 'reviewed with finance'\n  olcf governance certify 'Revenue mart' --kind product\n"
                                 "  olcf governance certify lakehouse_a.old --state deprecated")
@click.argument("subject")
@click.option("--kind", type=click.Choice(CERT_KINDS), default="asset", show_default=True)
@click.option("--state", "cert_state", type=click.Choice(CERT_STATES), default="certified", show_default=True)
@click.option("--comment", default=None)
@if_match_option
@json_option
@pass_state
def certify_cmd(state: State, subject: str, kind: str, cert_state: str, comment: str | None, if_match: str | None) -> None:
    doc = state.client.governance.set_certification(kind, resolve_subject(state, kind, subject), cert_state, comment=comment, if_match=if_match)  # type: ignore[arg-type]
    state.out.emit(doc, lambda d: state.out.success(f"{subject} is now {d.get('state')}"))


@governance_group.command("certification", help="The certification state of an asset, data product or term (and whether its schema changed since).",
                          epilog="\b\nExamples:\n  olcf governance certification lakehouse_a.mart\n  olcf governance certification 'Revenue mart' --kind product")
@click.argument("subject")
@click.option("--kind", type=click.Choice(CERT_KINDS), default="asset", show_default=True)
@json_option
@pass_state
def certification_cmd(state: State, subject: str, kind: str) -> None:
    doc = state.client.governance.certification(kind, resolve_subject(state, kind, subject))  # type: ignore[arg-type]
    state.out.emit(doc, lambda d: state.out.kv([("subject", f"{subject} ({kind})"), ("state", d.get("state")), ("certified by", d.get("certified_by")), ("certified at", d.get("certified_at")),
                                                ("comment", d.get("comment")), ("schema changed since", d.get("schema_changed_since")),
                                                ("invalidated", f"{d['invalidated_at']}: {d.get('invalidated_reason')}" if d.get("invalidated_at") else None), ("etag", doc.etag)]))


@governance_group.command("certifications", help="What is certified or deprecated (default), or in another state.", epilog="\b\nExamples:\n  olcf governance certifications\n  olcf governance certifications --state draft --kind asset")
@click.option("--state", "cert_state", type=click.Choice(CERT_STATES), default=None)
@click.option("--kind", type=click.Choice(CERT_KINDS), default=None)
@paging_options
@json_option
@pass_state
def certifications_cmd(state: State, cert_state: str | None, kind: str | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_certifications, limit=limit, fetch_all=fetch_all, state=cert_state, subject_type=kind)
    flat = [{**r, "name": G.asset_text(r.get("subject")) if r.get("subject") else r.get("subject_id")} for r in rows]
    state.out.table(flat if state.out.fmt not in ("json", "yaml") else rows,
                    [("SUBJECT", "name", 50), ("KIND", "subject_type"), ("STATE", "state"), ("CERTIFIED AT", "certified_at"), ("COMMENT", "comment", 40)], empty="nothing is certified or deprecated")


# --------------------------------------------------------------------------------------------- change requests
@click.group("requests", help="Change requests: propose a certification, deprecation, label, term link, ownership change or policy exception and have it approved.",
             epilog="\b\nExamples:\n  olcf governance requests create certify lakehouse_a.mart --comment 'please certify'\n  olcf governance requests list --status pending\n"
                    "  olcf governance requests approve chg_01... --comment ok")
def requests_group() -> None:
    pass


@requests_group.command("list", help="Change requests you can see.", epilog="\b\nExamples:\n  olcf governance requests list --status pending\n  olcf governance requests list --requester me")
@click.option("--status", type=click.Choice(["pending", "approved", "rejected", "withdrawn"]), default=None)
@click.option("--type", "type_", type=click.Choice(CHANGE_TYPES), default=None)
@click.option("--kind", "subject_kind", type=click.Choice([*SUBJECT_KINDS, "violation"]), default=None, help="Kind of the subject.")
@click.option("--subject", default=None, help="Subject (asset / object reference; needs --kind, a violation is given by id).")
@click.option("--requester", default=None, help="'me' or a user id.")
@click.option("--reviewer", default=None, help="'me' or a user id.")
@paging_options
@json_option
@pass_state
def requests_list(state: State, status: str | None, type_: str | None, subject_kind: str | None, subject: str | None, requester: str | None, reviewer: str | None, limit: int,
                  fetch_all: bool) -> None:
    if subject and not subject_kind:
        raise click.UsageError("--subject needs --kind")
    sid = (subject if subject_kind == "violation" else resolve_subject(state, subject_kind or "asset", subject)) if subject else None
    rows = all_pages(state.client.governance.list_changes, limit=limit, fetch_all=fetch_all, status=status, type=type_, subject_type=subject_kind, subject_id=sid, requester=requester,
                     reviewer=reviewer)
    state.out.table(G.change_rows(rows) if state.out.fmt not in ("json", "yaml") else rows,
                    [("ID", "id", 30), ("TYPE", "type"), ("SUBJECT", "subject", 44), ("STATUS", "status"), ("REQUESTER", "requester"), ("CREATED", "created_at")], empty="no change requests")


@requests_group.command("show", help="One change request with its payload and comments.", epilog="\b\nExample:\n  olcf governance requests show chg_01...")
@click.argument("change_id")
@json_option
@pass_state
def requests_show(state: State, change_id: str) -> None:
    doc = state.client.governance.get_change(change_id)

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.kv([("id", d.get("id")), ("type", d.get("type")), ("subject", f"{G.asset_text(d.get('subject'))} ({d.get('subject_type')})" if d.get("subject") else d.get("subject_id")),
              ("status", d.get("status")), ("requester", (d.get("requester") or {}).get("name")), ("payload", json.dumps(d.get("payload") or {}, sort_keys=True)),
              ("decided by", d.get("decided_by")), ("decided at", d.get("decided_at")), ("decision comment", d.get("decision_comment")), ("applied at", d.get("applied_at")),
              ("etag", doc.etag)])
        if d.get("comments"):
            o.text("")
            o.text("Comments:")
            for c in d["comments"]:
                o.text(f"  {c.get('at')}  {c.get('by_name') or c.get('by')}  [{c.get('action')}]  {c.get('text') or ''}".rstrip())

    state.out.emit(doc, human)


@requests_group.command("create", help="Open a change request; a reviewer with governance.approve applies it by approving. Payloads: certify {state}, deprecate {}, "
                                       "classification_change {add, remove}, term_link {term_id, action}, ownership_change {add, remove}, policy_exception {reason, expires_at}.",
                        epilog="\b\nExamples:\n  olcf governance requests create certify lakehouse_a.mart --comment 'please certify'\n"
                               "  olcf governance requests create classification_change lakehouse_a.raw --payload '{\"add\": [\"PII\"]}'\n"
                               "  olcf governance requests create policy_exception vio_01... --kind violation --set reason='scratch table'")
@click.argument("type_", metavar="TYPE", type=click.Choice(CHANGE_TYPES))
@click.argument("subject")
@click.option("--kind", "subject_kind", type=click.Choice([*SUBJECT_KINDS, "violation"]), default="asset", show_default=True, help="Kind of the subject (a violation is given by id).")
@click.option("--payload", default=None, help="Payload as a JSON object.")
@click.option("--payload-file", default=None, help="Payload JSON file (or - for stdin).")
@click.option("--set", "pairs", multiple=True, metavar="KEY=VALUE", help="One payload field (repeatable; values are JSON when they parse).")
@click.option("--reviewer", "reviewers", multiple=True, metavar="USER", help="Ask this user to review (repeatable).")
@click.option("--comment", default=None)
@json_option
@pass_state
def requests_create(state: State, type_: str, subject: str, subject_kind: str, payload: str | None, payload_file: str | None, pairs: tuple[str, ...], reviewers: tuple[str, ...],
                    comment: str | None) -> None:
    body = _load_json_object(payload, payload_file, pairs, "payload")
    sid = subject if subject_kind == "violation" else resolve_subject(state, subject_kind, subject)
    doc = state.client.governance.request_change(type_, subject_kind, sid, body, reviewers=list(reviewers) or None, comment=comment)  # type: ignore[arg-type]
    state.out.emit(doc, lambda d: state.out.success(f"opened {d['type']} request {d['id']} ({d['status']})"))


@requests_group.command("comment", help="Add a comment to a change request.", epilog="\b\nExample:\n  olcf governance requests comment chg_01... 'can you check the schema first?'")
@click.argument("change_id")
@click.argument("text")
@json_option
@pass_state
def requests_comment(state: State, change_id: str, text: str) -> None:
    state.out.emit(state.client.governance.comment_change(change_id, text), _ok(state, f"commented on {change_id}"))


@requests_group.command("approve", help="Approve and apply a change request (needs governance.approve; not your own request unless governance.allow_self_approval is on).",
                        epilog="\b\nExample:\n  olcf governance requests approve chg_01... --comment 'looks right'")
@click.argument("change_id")
@click.option("--comment", default=None)
@json_option
@pass_state
def requests_approve(state: State, change_id: str, comment: str | None) -> None:
    doc = state.client.governance.approve_change(change_id, comment=comment)
    state.out.emit(doc, lambda d: state.out.success(f"approved {change_id}" + (f" and applied it at {d['applied_at']}" if d.get("applied_at") else " (not applied yet: see `show`)")))


@requests_group.command("reject", help="Reject a change request; a reason is required.", epilog="\b\nExample:\n  olcf governance requests reject chg_01... --comment 'still in use'")
@click.argument("change_id")
@click.option("--comment", required=True, help="Why it is rejected.")
@json_option
@pass_state
def requests_reject(state: State, change_id: str, comment: str) -> None:
    state.out.emit(state.client.governance.reject_change(change_id, comment), _ok(state, f"rejected {change_id}"))


@requests_group.command("withdraw", help="Withdraw your own pending change request.", epilog="\b\nExample:\n  olcf governance requests withdraw chg_01...")
@click.argument("change_id")
@json_option
@pass_state
def requests_withdraw(state: State, change_id: str) -> None:
    state.out.emit(state.client.governance.withdraw_change(change_id), _ok(state, f"withdrew {change_id}"))


@requests_group.command("apply", help="Re-apply an approved change request whose application failed (idempotent; needs governance.approve).",
                        epilog="\b\nExample:\n  olcf governance requests apply chg_01...")
@click.argument("change_id")
@json_option
@pass_state
def requests_apply(state: State, change_id: str) -> None:
    doc = state.client.governance.apply_change(change_id)
    state.out.emit(doc, lambda d: state.out.success(f"{change_id}: {d.get('status')}" + (f", applied at {d['applied_at']}" if d.get("applied_at") else "")))


# --------------------------------------------------------------------------------------------- policies
@click.group("policies", help="Governance policies (observe mode): rules whose violations are reported; nothing is ever blocked.",
             epilog="\b\nExamples:\n  olcf governance policies rule-types\n  olcf governance policies create 'Tables have owners' --rule-type required_owner --param types='[\"table\"]' --severity high\n"
                    "  olcf governance policies evaluate")
def policies_group() -> None:
    pass


POLICY_COLS = [("ID", "id", 30), ("NAME", "name", 30), ("RULE", "rule_type", 24), ("SEVERITY", "severity"), ("MODE", "mode"), ("ENABLED", "enabled"), ("OPEN", "open"),
               ("LAST EVALUATED", "evaluated")]


@policies_group.command("rule-types", help="The closed set of rule types with their parameters.", epilog="\b\nExample:\n  olcf governance policies rule-types")
@json_option
@pass_state
def policies_rule_types(state: State) -> None:
    def human(rows: Any) -> None:
        for r in rows:
            state.out.text(f"{r['rule_type']}")
            state.out.text(f"  {r.get('description')}")
            for k, v in (r.get("params") or {}).items():
                state.out.text(f"    {k}: {v}")

    state.out.emit(list(state.client.governance.rule_types()), human)


@policies_group.command("list", help="Policies with their open violation counts.", epilog="\b\nExamples:\n  olcf governance policies list\n  olcf governance policies list --rule-type freshness --enabled")
@click.option("--rule-type", default=None)
@click.option("--enabled/--disabled", "enabled", default=None)
@paging_options
@json_option
@pass_state
def policies_list(state: State, rule_type: str | None, enabled: bool | None, limit: int, fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_policies, limit=limit, fetch_all=fetch_all, rule_type=rule_type, enabled=enabled)
    state.out.table(G.policy_rows(rows) if state.out.fmt not in ("json", "yaml") else rows, POLICY_COLS, empty="no policies yet (create one: `olcf governance policies create`)")


@policies_group.command("show", help="One policy with its parameters and its last evaluation.", epilog="\b\nExample:\n  olcf governance policies show 'Tables have owners'")
@click.argument("policy")
@json_option
@pass_state
def policies_show(state: State, policy: str) -> None:
    doc = state.client.governance.get_policy(resolve_named(state, "policy", policy))

    def human(d: dict[str, Any]) -> None:
        state.out.kv([("id", d.get("id")), ("name", d.get("name")), ("description", d.get("description")), ("rule", d.get("rule_type")), ("params", json.dumps(d.get("params") or {}, sort_keys=True)),
                      ("severity", d.get("severity")), ("mode", d.get("mode")), ("enabled", d.get("enabled")), ("open violations", d.get("open_violations")),
                      ("last evaluated", d.get("last_evaluated_at") or "never"), ("last result", json.dumps(d.get("last_result") or {}, sort_keys=True)), ("etag", doc.etag)])

    state.out.emit(doc, human)


def _policy_options(f: Callable[..., Any]) -> Callable[..., Any]:
    f = click.option("--severity", type=click.Choice(SEVERITIES), default=None)(f)
    f = click.option("--description", default=None)(f)
    f = click.option("--params-file", default=None, help="Parameters as a JSON object file (or - for stdin).")(f)
    return click.option("--param", "pairs", multiple=True, metavar="KEY=VALUE", help="One rule parameter (repeatable; values are JSON when they parse, e.g. types='[\"table\"]').")(f)


@policies_group.command("create", help="Create a policy (observe mode only: --mode enforce is refused with exit 7). Needs governance.approve.",
                        epilog="\b\nExamples:\n  olcf governance policies create 'Tables have owners' --rule-type required_owner --param types='[\"table\"]' --severity high\n"
                               "  olcf governance policies create 'PII stays out of reports' --rule-type classification_flow --params-file flow.json")
@click.argument("name")
@click.option("--rule-type", required=True, help="See `olcf governance policies rule-types`.")
@_policy_options
@click.option("--mode", default=None, help="Only 'observe' is supported.")
@click.option("--disabled", is_flag=True, help="Create it switched off.")
@json_option
@pass_state
def policies_create(state: State, name: str, rule_type: str, pairs: tuple[str, ...], params_file: str | None, description: str | None, severity: str | None, mode: str | None,
                    disabled: bool) -> None:
    params = _load_json_object(None, params_file, pairs, "params")
    doc = state.client.governance.create_policy(name, rule_type, params, description=description, severity=severity, mode=mode, enabled=False if disabled else None)  # type: ignore[arg-type]
    state.out.emit(doc, lambda d: state.out.success(f"created policy {d['name']} ({d['id']}), {d['severity']}, {d['mode']}; evaluate it with `olcf governance policies evaluate {d['id']}`"))


@policies_group.command("update", help="Change a policy (passing --param/--params-file replaces its parameters).", epilog="\b\nExamples:\n  olcf governance policies update 'Tables have owners' --severity critical\n  olcf governance policies update plc_01... --disabled")
@click.argument("policy")
@click.option("--name", "new_name", default=None)
@_policy_options
@click.option("--enabled/--disabled", "enabled", default=None)
@if_match_option
@json_option
@pass_state
def policies_update(state: State, policy: str, new_name: str | None, pairs: tuple[str, ...], params_file: str | None, description: str | None, severity: str | None,
                    enabled: bool | None, if_match: str | None) -> None:
    params = _load_json_object(None, params_file, pairs, "params") if (pairs or params_file) else None
    fields = _fields(name=new_name, params=params, description=description, severity=severity, enabled=enabled)
    doc = state.client.governance.update_policy(resolve_named(state, "policy", policy), if_match, **fields)
    state.out.emit(doc, lambda d: state.out.success(f"updated policy {d['name']} ({d['id']})"))


@policies_group.command("delete", help="Delete a policy and its violations.", epilog="\b\nExample:\n  olcf governance policies delete 'Tables have owners' --yes")
@click.argument("policy")
@if_match_option
@yes_option
@json_option
@pass_state
def policies_delete(state: State, policy: str, if_match: str | None, yes: bool) -> None:
    pid = resolve_named(state, "policy", policy)
    confirm(state, f"Delete policy {policy} and its violations?", yes)
    state.out.emit(state.client.governance.delete_policy(pid, if_match=if_match), _ok(state, f"deleted policy {policy}"))


@policies_group.command("evaluate", help="Evaluate one policy now, or every enabled policy. Evaluation is idempotent: a violation that is still true keeps its id.",
                        epilog="\b\nExamples:\n  olcf governance policies evaluate\n  olcf governance policies evaluate 'Tables have owners'")
@click.argument("policy", required=False)
@json_option
@pass_state
def policies_evaluate(state: State, policy: str | None) -> None:
    g = state.client.governance
    doc = g.evaluate_policy(resolve_named(state, "policy", policy)) if policy else g.evaluate_policies()

    def human(d: dict[str, Any]) -> None:
        results = d.get("results") if "results" in d else [d]
        state.out.table(results or [], [("POLICY", "policy_id", 30), ("EVALUATED", "evaluated"), ("OPENED", "opened"), ("UPDATED", "updated"), ("RESOLVED", "resolved"), ("REOPENED", "reopened"),
                                        ("UNKNOWN", "unknown"), ("MS", "duration_ms")], empty="no enabled policies")

    state.out.emit(doc, human)


@policies_group.command("preview", help="What a policy would report, without storing anything (needs governance.approve).",
                        epilog="\b\nExample:\n  olcf governance policies preview --rule-type required_description --param min_length=20 --param types='[\"table\"]'")
@click.option("--rule-type", required=True)
@click.option("--param", "pairs", multiple=True, metavar="KEY=VALUE")
@click.option("--params-file", default=None)
@click.option("--severity", type=click.Choice(SEVERITIES), default=None)
@click.option("--limit", type=click.IntRange(1, 500), default=20, show_default=True, help="How many findings to list.")
@json_option
@pass_state
def policies_preview(state: State, rule_type: str, pairs: tuple[str, ...], params_file: str | None, severity: str | None, limit: int) -> None:
    doc = state.client.governance.preview_policy(rule_type, _load_json_object(None, params_file, pairs, "params"), severity=severity, limit=limit)  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        state.out.text(f"{d.get('would_report')} violation(s) would be reported among {d.get('evaluated')} evaluated asset(s) ({d.get('unknown', 0)} unknown)")
        state.out.table([{"subject": G.asset_text(i.get("asset")) if i.get("asset") else i.get("subject_id"), "severity": i.get("severity"), "message": i.get("message")} for i in d.get("items") or []],
                        [("SUBJECT", "subject", 44), ("SEVERITY", "severity"), ("MESSAGE", "message", 80)], empty="nothing would be reported")
        for note in d.get("notes") or []:
            state.out.text(f"note: {note}")

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- violations
@click.group("violations", help="Policy violations (open and acknowledged by default).",
             epilog="\b\nExamples:\n  olcf governance violations list --severity high\n  olcf governance violations acknowledge vio_01... --comment 'known'")
def violations_group() -> None:
    pass


@violations_group.command("list", help="Violations of the assets you can see, worst first. Default status: open and acknowledged; --status any lists everything.",
                          epilog="\b\nExamples:\n  olcf governance violations list --severity high\n  olcf governance violations list --asset lakehouse_a.clean --status any")
@click.option("--policy", default=None, help="Only this policy (name or id).")
@click.option("--asset", default=None, help="Only this asset.")
@click.option("--severity", type=click.Choice(SEVERITIES), default=None)
@click.option("--status", default=None, help="open, acknowledged, excepted, resolved (comma separated) or any.")
@click.option("--rule-type", default=None)
@click.option("--workspace-id", default=None)
@paging_options
@json_option
@pass_state
def violations_list(state: State, policy: str | None, asset: str | None, severity: str | None, status: str | None, rule_type: str | None, workspace_id: str | None, limit: int,
                    fetch_all: bool) -> None:
    rows = all_pages(state.client.governance.list_violations, limit=limit, fetch_all=fetch_all, policy_id=resolve_named(state, "policy", policy) if policy else None,
                     asset_id=resolve_asset(state, asset) if asset else None, severity=severity, status=status, rule_type=rule_type, workspace_id=workspace_id)
    state.out.table(G.violation_rows(rows) if state.out.fmt not in ("json", "yaml") else rows,
                    [("ID", "id", 30), ("SEVERITY", "severity"), ("STATUS", "status"), ("POLICY", "policy", 28), ("SUBJECT", "subject", 40), ("MESSAGE", "message", 60)], empty="no violations")


@violations_group.command("show", help="One violation with its evidence.", epilog="\b\nExample:\n  olcf governance violations show vio_01...")
@click.argument("violation_id")
@json_option
@pass_state
def violations_show(state: State, violation_id: str) -> None:
    doc = state.client.governance.get_violation(violation_id)

    def human(d: dict[str, Any]) -> None:
        exc = d.get("exception")
        state.out.kv([("id", d.get("id")), ("policy", f"{(d.get('policy') or {}).get('name')} ({(d.get('policy') or {}).get('rule_type')})"),
                      ("subject", G.asset_text(d.get("asset")) if d.get("asset") else (d.get("product") or {}).get("name")), ("severity", d.get("severity")), ("status", d.get("status")),
                      ("message", d.get("message")), ("first seen", d.get("first_seen_at")), ("last seen", d.get("last_seen_at")), ("resolved at", d.get("resolved_at")),
                      ("acknowledged", f"{d['acknowledged_at']}: {d.get('ack_comment') or ''}".strip() if d.get("acknowledged_at") else None),
                      ("exception", f"{exc.get('reason')} (until {exc.get('until')}, change {exc.get('change_id')})" if exc else None),
                      ("evidence", json.dumps(d.get("evidence") or {}, sort_keys=True)), ("etag", doc.etag)])

    state.out.emit(doc, human)


@violations_group.command("acknowledge", help="Mark a violation as seen; it stays reported but is no longer 'open'.", epilog="\b\nExample:\n  olcf governance violations acknowledge vio_01... --comment 'known'")
@click.argument("violation_id")
@click.option("--comment", default=None)
@json_option
@pass_state
def violations_ack(state: State, violation_id: str, comment: str | None) -> None:
    state.out.emit(state.client.governance.acknowledge_violation(violation_id, comment=comment), _ok(state, f"acknowledged {violation_id}"))


@violations_group.command("reopen", help="Reopen an acknowledged or excepted violation.", epilog="\b\nExample:\n  olcf governance violations reopen vio_01...")
@click.argument("violation_id")
@json_option
@pass_state
def violations_reopen(state: State, violation_id: str) -> None:
    state.out.emit(state.client.governance.reopen_violation(violation_id), _ok(state, f"reopened {violation_id}"))


# --------------------------------------------------------------------------------------------- quality
@governance_group.command("quality", help="Quality score of an asset or data product with the explainable breakdown (documentation, ownership, classification, freshness, certification, lineage, violations, schema stability).",
                          epilog="\b\nExamples:\n  olcf governance quality lakehouse_a.clean\n  olcf governance quality 'Revenue mart' --product")
@click.argument("subject")
@click.option("--product", "is_product", is_flag=True, help="SUBJECT is a data product.")
@click.option("--type", "asset_type", default=None, help="Only consider assets of this type when resolving SUBJECT.")
@json_option
@pass_state
def quality_cmd(state: State, subject: str, is_product: bool, asset_type: str | None) -> None:
    kind = "product" if is_product else "asset"
    doc = state.client.governance.quality(kind, resolve_subject(state, kind, subject, asset_type))  # type: ignore[arg-type]

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.text(f"Quality of {subject}: {d.get('score')} / 100" + (f"  (own {d.get('own_score')})" if d.get("own_score") is not None and d.get("own_score") != d.get("score") else ""))
        for line in G.quality_lines({"components": d.get("components") or d.get("product_components")}):
            o.text(line)
        roll = (d.get("rollup") or {}).get("columns")
        if roll:
            o.text(f"  columns: {roll.get('count')} column(s), mean {roll.get('mean')} (weight {(d.get('rollup') or {}).get('column_weight')})")
        if d.get("members"):
            o.text("  members: " + ", ".join(f"{G.asset_text(m['asset'])} {m['score']}" for m in d["members"]))

    state.out.emit(doc, human)


def register(group: click.Group) -> None:
    """Attach ``olcf governance`` and its subgroups to the root command group."""
    for sub in (terms_group, domains_group, products_group, owners_group, classifications_group, review_group, requests_group, policies_group, violations_group):
        governance_group.add_command(sub)
    group.add_command(governance_group)
