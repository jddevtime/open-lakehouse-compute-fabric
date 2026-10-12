# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP tools of OLCF governance: what a business term means, who owns an asset and whether it is certified, where sensitive data ends up, and how good the data is.

Like the Intelligence tools these are **read-only** (ADR 0014: the ``mcp`` token scope carries ``intelligence.read`` and nothing that writes - no ``governance.curate``, no
``governance.approve``) and call the governance managers with the caller's own principal, so every visibility rule of ``/v1/governance/*`` applies unchanged: an asset outside the
caller's workspaces answers ``not_found``, and evidence that passes through such an asset reads ``(hidden)``.  There is no tool that creates, approves, certifies, classifies or
evaluates anything, and none that returns sample values: classification evidence is rule names and lineage paths, never data.

The tools are listed only while the governance package is installed (``Tool.service == "intelligence.governance"``).  Answers are compact and bounded exactly like
:mod:`olcf.mcp.intel_tools` (``max_*`` arguments with hard ceilings, ``truncated`` and ``omitted`` counts, a ``summary`` sentence an agent can quote), and every statement about
lineage carries its confidence: a *proven* exposure follows column-level or table-level lineage that was seen, a *possible* one only table-level lineage to a column's table.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from olcf.core.errors import BadRequest, NotFound, Unavailable
from olcf.mcp.intel_tools import (
    ASSET_ARG,
    CONF_SCHEMA,
    TYPE_ARG,
    WORKSPACE_ARG,
    _svc,
    _touch,
    _ws_name,
    cap,
    clip,
    ref,
    resolve_asset,
)
from olcf.mcp.protocol import Tool, obj

if TYPE_CHECKING:
    from olcf.mcp.tools import Env

HIDDEN = "(hidden)"
CERT_STATES = ("none", "draft", "certified", "deprecated")


# ----------------------------------------------------------------------------------------------------------------------------------- helpers
def _gov(env: Env) -> Any:
    """The governance package of the Intelligence service, or ``Unavailable`` when it is not installed."""
    gov = _svc(env).governance
    if gov is None:
        raise Unavailable("OLCF governance is not enabled on this server", remediation="ask an administrator to install a build that includes olcf.intelligence.governance")
    return gov


def _name(x: Mapping[str, Any] | None, labels: Mapping[str, str] | None = None) -> str:
    """Readable name of an asset reference from governance evidence; an asset the caller cannot see is ``(hidden)`` and never named."""
    if not x:
        return "-"
    if x.get("hidden"):
        return HIDDEN
    if labels and x.get("id") in labels:
        return labels[str(x["id"])]
    return str(x.get("path") or x.get("name") or x.get("id") or "-")


def _column_labels(env: Env, refs: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """``{column id: "table.column"}`` for the visible columns among ``refs`` (a compact column reference has no path, so ``amount`` alone would be ambiguous)."""
    svc = _svc(env)
    cols = {str(r["id"]): r for r in refs if r.get("type") == "column" and r.get("id") and not r.get("hidden")}
    if not cols:
        return {}
    rows = svc.graph.asset_rows(list(cols)[:200])
    parents = svc.graph.asset_rows(sorted({r["parent_id"] for r in rows.values() if r.get("parent_id")}))
    out = {}
    for cid, row in rows.items():
        parent = parents.get(row.get("parent_id") or "")
        if parent is not None and svc.can_see(env.principal, parent["workspace_id"]):
            out[cid] = f"{parent['name']}.{row['name']}"
    return out


def _refs(doc: Any) -> list[Mapping[str, Any]]:
    """Every asset reference (a mapping with ``id`` and ``type``) found anywhere in ``doc``."""
    found: list[Mapping[str, Any]] = []

    def visit(x: Any) -> None:
        if isinstance(x, Mapping):
            if x.get("id") and x.get("type") and not x.get("hidden"):
                found.append(x)
            for v in x.values():
                visit(v)
        elif isinstance(x, (list, tuple)):
            for v in x:
                visit(v)

    visit(doc)
    return found


def _owner(o: Mapping[str, Any]) -> dict[str, Any]:
    """Compact owner: name, role and (for an inherited owner) where it comes from.  E-mail addresses are left out."""
    out: dict[str, Any] = {"name": o.get("name"), "role": o.get("role")}
    if o.get("kind") == "external":
        out["external"] = True
    inherited = o.get("inherited_from")
    if isinstance(inherited, Mapping) and inherited.get("name"):
        out["inherited_from"] = inherited["name"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- governance_glossary_search
def governance_glossary_search(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Ranked search of the business glossary, each term with its definition, status, owners and a few of the assets it is linked to."""
    gov, p = _gov(env), env.principal
    limit = cap(a.get("limit"), 8, 25)
    per_term = cap(a.get("assets_per_term"), 3, 10)
    hits = gov.glossary.search(p, a["query"], limit=limit + 1, include_deprecated=bool(a.get("include_deprecated")))
    more = len(hits) > limit
    terms = []
    for h in hits[:limit]:
        t = h["term"]
        links, _ = gov.glossary.links_of_term(p, t["id"], status=None, limit=per_term, cursor=None)
        item: dict[str, Any] = {"id": t["id"], "name": t["name"], "status": t["status"], "certification": t.get("certification"), "definition": clip(t.get("definition") or "", 500),
                                "matched_on": h.get("matched_on"), "score": h.get("score")}
        if t.get("abbreviation"):
            item["abbreviation"] = t["abbreviation"]
        if t.get("synonyms"):
            item["synonyms"] = t["synonyms"][:8]
        item["owners"] = [_owner(o) for o in (t.get("owners") or [])[:5]]
        examples = [{**ref(lk["asset"]), "link_status": lk.get("status")} for lk in links if lk.get("asset")]
        item["linked_assets"] = {"count": t.get("link_count", 0), "examples": examples, "truncated": int(t.get("link_count") or 0) > len(examples)}
        _touch(env, *(lk["asset"] for lk in links if lk.get("asset")))
        terms.append(item)
    out: dict[str, Any] = {"query": a["query"], "count": len(terms), "terms": terms, "truncated": more}
    if terms:
        best = terms[0]
        out["summary"] = (f"'{best['name']}' ({best['status']}, matched on {best['matched_on']}): {clip(best['definition'], 160) or 'no definition yet'}"
                          + (f" - {len(terms) - 1} more term(s) match." if len(terms) > 1 else ""))
    else:
        out["summary"] = f"no glossary term matches '{a['query']}'"
        out["note"] = "try a shorter word, a synonym or an abbreviation; deprecated terms are searched with include_deprecated=true"
    if more:
        out["note"] = f"more than {limit} terms match; narrow the query"
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- governance_asset
def governance_asset(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Everything governance knows about one asset: domain, data products, glossary terms, effective owners, classifications with their evidence, certification, quality, violations."""
    gov = _gov(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    d = gov.asset_detail(env.principal, found["id"])
    asset = d.get("asset") or found
    max_v = cap(a.get("max_violations"), 10, 50)
    cls = d.get("classifications") or {}
    labels = _column_labels(env, _refs(cls))

    def assigned(rows: Sequence[Mapping[str, Any]], with_path: bool = False) -> list[dict[str, Any]]:
        out = []
        for r in rows[:20]:
            item: dict[str, Any] = {"label": (r.get("classification") or {}).get("name"), "status": r.get("status"), "source": r.get("source")}
            if r.get("confidence") is not None:
                item["confidence"] = r["confidence"]
            if r.get("precision"):
                item["precision"] = r["precision"]
            if with_path:
                ev = r.get("evidence") or {}
                item["from"] = _name(ev.get("from_asset"), labels)
                item["via_hops"] = max(len((ev.get("path") or {}).get("nodes") or []) - 1, 0)
            out.append(item)
        return out

    owners = (d.get("owners") or {}).get("effective") or []
    violations = list(d.get("violations") or [])
    cert = d.get("certification") or {}
    dom = d.get("domain")
    out: dict[str, Any] = {"asset": ref(asset), "workspace": _ws_name(env, {}, asset.get("workspace_id"))}
    out["domain"] = {"name": dom["domain"]["name"], "id": dom["domain"]["id"], "via": dom.get("source")} if dom else None
    out["data_products"] = [{"id": x["id"], "name": x["name"], "status": x.get("status")} for x in (d.get("products") or [])[:10]]
    out["glossary_terms"] = [{"id": x["id"], "name": x["name"], "status": x.get("status")} for x in (d.get("terms") or [])[:15]]
    out["pending_term_links"] = len(d.get("pending_term_links") or [])
    out["owners"] = [_owner(o) for o in owners[:10]]
    out["classifications"] = {"direct": assigned(cls.get("direct") or []), "inherited": assigned(cls.get("inherited") or [], True), "suggested": assigned(cls.get("suggestions") or []),
                              "contained_in_columns": [{"label": (c.get("classification") or {}).get("name"), "columns": c.get("columns")} for c in (cls.get("contained") or [])[:10]]}
    out["certification"] = {"state": cert.get("state", "none"), "certified_at": cert.get("certified_at"), "comment": clip(cert.get("comment"), 200),
                            "invalidated_reason": cert.get("invalidated_reason"), "schema_changed_since": cert.get("schema_changed_since")}
    out["quality_score"] = (d.get("quality") or {}).get("score")
    out["violations"] = {"count": len(violations), "items": [{"id": v["id"], "policy": (v.get("policy") or {}).get("name"), "rule_type": (v.get("policy") or {}).get("rule_type"),
                                                             "severity": v.get("severity"), "status": v.get("status"), "message": clip(v.get("message"), 200)} for v in violations[:max_v]]}
    out["open_change_requests"] = [{"id": c["id"], "type": c.get("type"), "status": c.get("status")} for c in (d.get("open_changes") or [])[:10]]
    out["truncated"] = len(violations) > max_v
    facts = []
    facts.append(f"owned by {', '.join(o['name'] for o in out['owners'][:3])}" if out["owners"] else "no owner is assigned (direct or inherited)")
    facts.append(f"certification: {out['certification']['state']}")
    labelled = sorted({c["label"] for bucket in ("direct", "inherited") for c in out["classifications"][bucket] if c["label"]})
    facts.append(f"labelled {', '.join(labelled)}" if labelled else "no classification label")
    if out["domain"]:
        facts.append(f"domain {out['domain']['name']}")
    facts.append(f"{len(violations)} open policy violation(s)" if violations else "no open policy violations")
    out["summary"] = f"{asset.get('path') or asset.get('name')} ({asset.get('type')}): " + "; ".join(facts) + "."
    out["next"] = ["governance_exposure", "governance_quality", "intel_lineage", "intel_impact_analysis"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- governance_exposure
def governance_exposure(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Where does data carrying a classification label end up: reports, visuals, semantic models, tables, data products - with proven / possible and the lineage path."""
    gov = _gov(env)
    cls_ref = a["classification"].strip()
    if not cls_ref:
        raise BadRequest("classification must not be empty", remediation="pass a label such as 'PII', 'PCI' or 'Restricted' (see the classification catalog)")
    ws = env.workspace(a["workspace"], "intelligence.read")["id"] if a.get("workspace") else None
    types = a.get("types") or None
    doc = gov.classifications.exposure(env.principal, cls_ref, depth=a.get("depth"), min_confidence=a.get("min_confidence"), include_possible=a.get("include_possible", True),
                                       types=",".join(types) if types else None, workspace_id=ws)
    cls = doc.get("classification") or {}
    max_items = cap(a.get("max_items"), 25, 100)
    max_path = 10
    exposures = list(doc.get("exposures") or [])
    shown = exposures[:max_items]
    labels = _column_labels(env, _refs([doc.get("sources"), shown]))
    items = []
    for e in shown:
        nodes = (e.get("path") or {}).get("nodes") or []
        asset = e.get("asset") or {}
        item = {**ref(asset, _name(asset, labels)), "kind": e.get("kind"), "proven": bool(e.get("proven")), "precision": e.get("precision"), "confidence": e.get("confidence_floor"),
                "from": _name(e.get("from_asset"), labels), "hops": max(len(nodes) - 1, 0), "path": [_name(n, labels) for n in nodes[:max_path]]}
        if len(nodes) > max_path:
            item["path_truncated"] = True
        items.append(item)
    _touch(env, *(e.get("asset") or {} for e in shown))
    summary = doc.get("summary") or {}
    sources = doc.get("sources") or []
    reports = int(summary.get("reports", 0))
    by_type = summary.get("by_type") or {}
    parts = [f"{summary.get('proven', 0)} proven and {summary.get('possible', 0)} possible exposure(s)"]
    if by_type:
        parts.append("by type: " + ", ".join(f"{n} {t}" for t, n in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0]))))
    out: dict[str, Any] = {
        "classification": {"id": cls.get("id"), "name": cls.get("name"), "category": cls.get("category"), "level": cls.get("level")},
        "summary": f"Data labelled {cls.get('name')} reaches {len(exposures)} asset(s): " + "; ".join(parts) + f". Reports: {reports}.",
        "labelled_sources": [{**ref(s["asset"], _name(s["asset"], labels))} if not s["asset"].get("hidden") else {"hidden": True} for s in sources[:10]],
        "labelled_source_count": len(sources), "counts": {"proven": summary.get("proven", 0), "possible": summary.get("possible", 0), "reports": reports,
                                                          "visuals": summary.get("visuals", 0), "semantic_models": summary.get("semantic_models", 0), "data_products": summary.get("products", 0),
                                                          "by_type": by_type},
        "exposures": items, "data_products": [{"id": x["product"]["id"], "name": x["product"]["name"], "status": x["product"].get("status")} for x in (doc.get("data_products") or [])[:10]],
        "truncated": bool(doc.get("truncated")) or len(exposures) > len(items), "omitted": {"exposures": max(0, len(exposures) - len(items))},
        "notes": [clip(n, 300) for n in doc.get("notes") or []]}
    out["complete"] = not out["truncated"]
    if not exposures:
        out["notes"].append("nothing downstream of the labelled assets matched; lower min_confidence, raise depth or drop the types filter to look wider")
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- governance_quality
def _product(env: Env, text: str) -> dict[str, Any]:
    gov, p = _gov(env), env.principal
    text = text.strip()
    if text.startswith("dpr_"):
        return dict(gov.products.get(p, text))
    rows, _ = gov.products.list(p, q=text, limit=50)
    hits = [r for r in rows if str(r.get("name") or "").lower() == text.lower()]
    if not hits:
        raise NotFound(f"no data product named '{text}' is visible to you" + (f"; closest: {', '.join(r['name'] for r in rows[:5])}" if rows else ""),
                       remediation="call governance_asset on one of its assets, or pass the product id (dpr_...)")
    if len(hits) > 1:
        raise BadRequest(f"'{text}' names {len(hits)} data products: " + ", ".join(f"{h['name']} ({h['id']})" for h in hits[:6]), remediation="pass the product id (dpr_...)")
    return dict(hits[0])


def governance_quality(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Quality score of an asset (or data product) with the explainable breakdown: which components lost points and why."""
    gov, p = _gov(env), env.principal
    if bool(a.get("asset")) == bool(a.get("product")):
        raise BadRequest("give exactly one of asset or product", remediation="governance_quality asset='sales' or governance_quality product='Revenue mart'")
    max_reasons = cap(a.get("max_reasons"), 3, 10)
    if a.get("product"):
        prod = _product(env, a["product"])
        d = gov.quality.product(p, prod["id"])
        subject: dict[str, Any] = {"id": prod["id"], "type": "data_product", "name": prod["name"]}
        components = d.get("product_components") or []
        extra: dict[str, Any] = {"members": [{**ref(m["asset"]), "score": m.get("score")} for m in (d.get("members") or [])[:15]], "member_count": len(d.get("members") or []),
                                 "hidden_members": d.get("hidden_members", 0), "member_mean": d.get("member_mean"), "own_score": d.get("own_score"), "truncated": len(d.get("members") or []) > 15}
    else:
        found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
        d = gov.quality.for_asset(p, found["id"])
        subject = ref(found)
        components = d.get("components") or []
        roll = (d.get("rollup") or {}).get("columns")
        extra = {"own_score": d.get("own_score"), "truncated": False}
        if roll:
            extra["columns"] = {"count": roll.get("count"), "mean": roll.get("mean"), "weight": (d.get("rollup") or {}).get("column_weight")}
        _touch(env, found)
    rows = []
    for c in components:
        if not c.get("applicable"):
            rows.append({"component": c["name"], "applicable": False})
            continue
        lost = round(float(c.get("max_points") or 0) - float(c.get("points") or 0), 2)
        rows.append({"component": c["name"], "applicable": True, "points": c.get("points"), "max_points": c.get("max_points"), "points_lost": lost,
                     "reasons": [clip(r.get("reason"), 200) for r in (c.get("reasons") or [])[:max_reasons]]})
    worst = sorted((r for r in rows if r.get("applicable") and r["points_lost"] > 0), key=lambda r: -r["points_lost"])[:3]
    name = subject.get("name")
    summary = f"{name} scores {d.get('score')} out of 100"
    if worst:
        losses = "; ".join(f"{r['component']} (-{r['points_lost']:g}: {r['reasons'][0] if r['reasons'] else 'see reasons'})" for r in worst)
        summary += f"; the biggest losses: {losses}."
    else:
        summary += "; no component lost points."
    return {"subject": subject, "score": d.get("score"), "summary": summary, "components": rows, "computed_at": d.get("computed_at"), **extra}


# ----------------------------------------------------------------------------------------------------------------------------------- registry
def build_gov_tools() -> list[Tool]:
    """The read-only governance tools (listed only to callers with ``intelligence.read`` and only while the governance package is installed)."""
    need = ("intelligence.read",)
    limit = lambda default, top: {"type": "integer", "minimum": 1, "maximum": top, "default": default}  # noqa: E731
    tools = [
        Tool("governance_glossary_search", "Search the business glossary",
             "What does a business word mean here? Ranked search over the glossary (names, synonyms, abbreviations, definitions) returning each term with its definition, status "
             "(draft, approved, deprecated), certification, owners and some of the tables or reports it is linked to. Questions: 'What does active customer mean?', 'Which tables implement "
             "the Revenue term?', 'Is there an official definition of churn?'. Combine with intel_search_assets to find the data.",
             obj({"query": {"type": "string", "minLength": 1, "maxLength": 200, "description": "A word or phrase: a term name, synonym, abbreviation or part of a definition"},
                  "limit": limit(8, 25), "assets_per_term": limit(3, 10), "include_deprecated": {"type": "boolean", "default": False}}, ["query"]), governance_glossary_search, needs=need),
        Tool("governance_asset", "Governance facts of an asset",
             "Who owns this and is it certified? Everything governance knows about one asset: domain, data products it belongs to, glossary terms, effective owners and stewards (inherited "
             "from parents), classification labels (direct, inherited along lineage with confidence, suggested, contained in columns), certification state, quality score, open policy "
             "violations and pending change requests. Questions: 'Who owns the orders table?', 'Is sales_report certified?', 'Does this table contain PII?', 'Which policies does it violate?'.",
             obj({"asset": ASSET_ARG, "type": TYPE_ARG, "workspace": WORKSPACE_ARG, "max_violations": limit(10, 50)}, ["asset"]), governance_asset, needs=need),
        Tool("governance_exposure", "Where does labelled data end up?",
             "Follow data carrying a classification label (PII, PCI, Financial, Credentials, Restricted or a custom label) down the lineage to the reports, visuals, semantic models, tables "
             "and data products that receive it, each marked proven (column-level or seen table-level lineage) or possible (only the column's table feeds it), with the lineage path as "
             "evidence and its confidence. Questions: 'Which reports show PII?' (types=['report']), 'Where does credit card data end up?', 'Is restricted data reaching a dashboard?'. "
             "Lineage that was not discovered cannot be followed: read notes and truncated.",
             obj({"classification": {"type": "string", "minLength": 1, "maxLength": 128, "description": "Label name or id, e.g. PII"},
                  "types": {"type": "array", "items": {"type": "string", "maxLength": 32}, "maxItems": 12, "description": "Only these asset types: report, dashboard, visual, semantic_model, table, view, measure ..."},
                  "min_confidence": CONF_SCHEMA, "include_possible": {"type": "boolean", "default": True, "description": "Also list possible exposures (table-level lineage only)"},
                  "depth": {"type": "integer", "minimum": 1, "maximum": 10, "description": "Lineage hops (default: the server setting)"}, "workspace": WORKSPACE_ARG, "max_items": limit(25, 100)},
                 ["classification"]), governance_exposure, needs=need),
        Tool("governance_quality", "Quality score and why",
             "How good is this data? The governance quality score (0-100) of an asset or data product with the explainable breakdown: documentation, ownership, classification, freshness, "
             "certification, lineage, policy violations and schema stability - each with the points lost and the reasons. Questions: 'How good is the quality of the sales table?', "
             "'What would improve the Revenue mart score?', 'Why is this table rated low?'.",
             obj({"asset": ASSET_ARG, "type": TYPE_ARG, "workspace": WORKSPACE_ARG, "product": {"type": "string", "minLength": 1, "maxLength": 200, "description": "A data product name or id (dpr_...) instead of an asset"},
                  "max_reasons": limit(3, 10)}), governance_quality, needs=need),
    ]
    for t in tools:
        t.service = "intelligence.governance"
    return tools
