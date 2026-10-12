# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure renderers of OLCF governance answers for the terminal (no I/O, no click): table rows, one-line texts and the exposure / quality / asset views.

Every function takes the decoded JSON of ``/v1/governance/*`` and returns strings or table rows, so ``olcf governance`` stays a thin layer and these shapes are testable on
their own.  An asset the caller cannot see appears in evidence as ``{"hidden": true}``; it is rendered as ``(hidden)`` and never named.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

HIDDEN = "(hidden)"


def asset_text(ref: Mapping[str, Any] | None, labels: Mapping[str, str] | None = None) -> str:
    """``path`` (or the name) of an asset reference, ``(hidden)`` for an asset the caller cannot see, ``-`` for none.

    ``labels`` maps an asset id to a better name (``clean.amount`` for a column, whose reference carries no path).
    """
    if not ref:
        return "-"
    if ref.get("hidden"):
        return HIDDEN
    if labels and ref.get("id") in labels:
        return labels[str(ref["id"])]
    return str(ref.get("path") or ref.get("name") or ref.get("id") or "-")


def column_refs(*docs: Any) -> list[dict[str, Any]]:
    """Every column reference found in exposure documents, assignment lists or evidence paths (so their tables can be looked up and shown as ``table.column``)."""
    found: dict[str, dict[str, Any]] = {}

    def visit(x: Any) -> None:
        if isinstance(x, Mapping):
            if x.get("type") == "column" and x.get("id") and not x.get("hidden"):
                found[str(x["id"])] = dict(x)
            for v in x.values():
                visit(v)
        elif isinstance(x, (list, tuple)):
            for v in x:
                visit(v)

    for d in docs:
        visit(d)
    return list(found.values())


def owners_text(owners: Sequence[Mapping[str, Any]] | None) -> str:
    """``Ann (owner), Data Office (steward, inherited from raw)``; ``none`` without owners."""
    parts = []
    for o in owners or []:
        inherited = o.get("inherited_from")
        suffix = f", inherited from {inherited.get('name')}" if isinstance(inherited, Mapping) and inherited.get("name") else ""
        parts.append(f"{o.get('name')} ({o.get('role')}{suffix})")
    return ", ".join(parts) or "none"


def classes_text(assignments: Sequence[Mapping[str, Any]] | None) -> str:
    """Label names of a list of assignments (``PII, Internal``); ``none`` when empty."""
    return ", ".join(sorted({str((a.get("classification") or {}).get("name")) for a in assignments or []})) or "none"


def term_rows(terms: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for the glossary table."""
    return [{"id": t.get("id"), "name": t.get("name"), "abbreviation": t.get("abbreviation") or "", "status": t.get("status"), "certification": t.get("certification"),
             "links": t.get("link_count"), "definition": " ".join(str(t.get("definition") or "").split())} for t in terms]


def search_rows(hits: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for glossary search hits (ranked): ``score`` and what matched next to the term."""
    return [{"id": (h.get("term") or {}).get("id"), "name": (h.get("term") or {}).get("name"), "status": (h.get("term") or {}).get("status"), "score": h.get("score"),
             "matched": h.get("matched_on"), "definition": " ".join(str((h.get("term") or {}).get("definition") or "").split())} for h in hits]


def link_rows(links: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for term links: the linked asset or product with the status, source and confidence of the link."""
    rows = []
    for lk in links:
        target = lk.get("asset") or lk.get("product") or {}
        rows.append({"id": lk.get("id"), "term": (lk.get("term") or {}).get("name"), "kind": lk.get("subject_type"), "target": asset_text(target) if lk.get("asset") else target.get("name"),
                     "status": lk.get("status"), "source": lk.get("source"), "confidence": lk.get("confidence")})
    return rows


def review_rows(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for the review queue: what is proposed (a label or a term) for which asset, how sure and who may decide."""
    rows = []
    for i in items:
        what = (i.get("classification") or {}).get("name") or (i.get("term") or {}).get("name") or ""
        rows.append({"id": i.get("id"), "kind": i.get("kind"), "asset": asset_text(i.get("asset")), "proposal": what, "confidence": i.get("confidence"), "source": i.get("source"),
                     "requires": i.get("requires"), "created_at": i.get("created_at")})
    return rows


def change_rows(changes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for change requests (``olcf governance requests list``)."""
    return [{"id": c.get("id"), "type": c.get("type"), "subject": asset_text(c.get("subject")) if c.get("subject") else c.get("subject_id"), "status": c.get("status"),
             "requester": (c.get("requester") or {}).get("name"), "created_at": c.get("created_at")} for c in changes]


def violation_rows(violations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for policy violations, worst severity first."""
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    rows = [{"id": v.get("id"), "severity": v.get("severity"), "status": v.get("status"), "policy": (v.get("policy") or {}).get("name"),
             "subject": asset_text(v.get("asset")) if v.get("asset") else (v.get("product") or {}).get("name"), "message": v.get("message"), "last_seen_at": v.get("last_seen_at")}
            for v in violations]
    rows.sort(key=lambda r: (order.get(str(r["severity"]), 9), str(r["subject"])))
    return rows


def policy_rows(policies: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows for policies: rule, severity, mode, open violations and when it was last evaluated."""
    return [{"id": p.get("id"), "name": p.get("name"), "rule_type": p.get("rule_type"), "severity": p.get("severity"), "mode": p.get("mode"), "enabled": p.get("enabled"),
             "open": p.get("open_violations"), "evaluated": p.get("last_evaluated_at") or "never"} for p in policies]


def exposure_rows(doc: Mapping[str, Any], labels: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    """Rows of a classification exposure: every asset the labelled data reaches, proven ones first, with the asset it came from and the lineage path."""
    rows = []
    for e in doc.get("exposures") or []:
        nodes = (e.get("path") or {}).get("nodes") or []
        rows.append({"asset": asset_text(e.get("asset"), labels), "type": (e.get("asset") or {}).get("type"), "precision": e.get("precision"), "proven": "yes" if e.get("proven") else "possible",
                     "confidence": e.get("confidence_floor"), "from": asset_text(e.get("from_asset"), labels), "hops": max(len(nodes) - 1, 0), "id": (e.get("asset") or {}).get("id")})
    return rows


def exposure_paths(doc: Mapping[str, Any], labels: Mapping[str, str] | None = None) -> list[str]:
    """One line per exposed asset: ``raw.amount -> etl -> clean.amount  [derived, proven]`` (hidden path members shown as ``(hidden)``)."""
    out = []
    for e in doc.get("exposures") or []:
        nodes = (e.get("path") or {}).get("nodes") or []
        chain = " -> ".join(HIDDEN if n.get("hidden") else (labels or {}).get(str(n.get("id")), str(n.get("name") or n.get("id"))) for n in nodes) or asset_text(e.get("asset"), labels)
        out.append(f"{chain}  [{e.get('confidence_floor')}, {'proven' if e.get('proven') else 'possible'}]")
    return out


def exposure_headline(doc: Mapping[str, Any]) -> str:
    """``PII reaches 5 asset(s): 2 proven, 3 possible; 1 report(s), 1 semantic model(s), 0 data product(s).``"""
    s = doc.get("summary") or {}
    name = (doc.get("classification") or {}).get("name") or "the label"
    total = int(s.get("proven", 0)) + int(s.get("possible", 0))
    return (f"Data labelled {name} reaches {total} asset(s): {s.get('proven', 0)} proven, {s.get('possible', 0)} possible; {s.get('reports', 0)} report(s), "
            f"{s.get('semantic_models', 0)} semantic model(s), {s.get('products', 0)} data product(s).")


def quality_lines(doc: Mapping[str, Any]) -> list[str]:
    """The explainable breakdown of a quality score: one line per applicable component with the reasons points were lost."""
    out = []
    for c in doc.get("components") or []:
        if not c.get("applicable"):
            out.append(f"  {str(c.get('name')).ljust(18)} n/a (does not apply)")
            continue
        lost = "; ".join(f"-{r.get('points_lost'):g} {r.get('reason')}" for r in c.get("reasons") or [])
        out.append((f"  {str(c.get('name')).ljust(18)} {c.get('points'):>5g} / {c.get('max_points'):<5g}" + (f"  {lost}" if lost else "")).rstrip())
    return out


def summary_pairs(d: Mapping[str, Any]) -> list[tuple[str, Any]]:
    """Key / value pairs of the governance overview."""
    cov, rq, vio = d.get("coverage") or {}, d.get("review_queue") or {}, d.get("violations") or {}
    cls = d.get("classifications") or {}
    return [("glossary terms", f"{(d.get('glossary') or {}).get('terms', 0)}  ({_counts((d.get('glossary') or {}).get('by_status'))})"),
            ("domains", d.get("domains", 0)),
            ("data products", f"{(d.get('products') or {}).get('total', 0)}  ({_counts((d.get('products') or {}).get('by_status'))})"),
            ("classifications", f"{cls.get('labels', 0)} labels; assignments {_counts(cls.get('assignments'))}"),
            ("review queue", f"{rq.get('total', 0)} pending  (classifications {rq.get('classification', 0)}, term links {rq.get('term_link', 0)}); {rq.get('change_requests', 0)} change request(s)"),
            ("open violations", f"{vio.get('open', 0)}  ({_counts(vio.get('by_severity'))})"),
            ("certification", _counts((d.get("certification") or {}).get("by_state"))),
            ("coverage of datasets", f"{cov.get('datasets', 0)} datasets: owner {cov.get('owner_pct')}%, description {cov.get('description_pct')}%, domain {cov.get('domain_pct')}%, "
                                     f"term {cov.get('term_pct')}%, classification reviewed {cov.get('classification_reviewed_pct')}%, certified {cov.get('certified_pct')}%"),
            ("last policy evaluation", (d.get("evaluation") or {}).get("last_evaluated_at") or "never")]


def _counts(d: Mapping[str, Any] | None) -> str:
    return ", ".join(f"{k} {v}" for k, v in (d or {}).items()) or "none"
