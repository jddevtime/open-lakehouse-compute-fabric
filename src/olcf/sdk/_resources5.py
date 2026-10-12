# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Wrappers of OLCF Intelligence governance: business glossary, domains, data products, owners, classifications, review queue, certification, change requests, policies,
violations and quality (``client.governance``).

The REST contract is ``docs/dev/GOVERNANCE_API.md``.  Same conventions as :mod:`olcf.sdk._resources`: JSON objects come back as :class:`~olcf.sdk._core.Resource` (with
``.etag`` for the mutable ones - pass the resource you read as ``if_match=`` to update or delete it without losing a concurrent change), lists as
:class:`~olcf.sdk._core.Page`.  Governance **observes and reports**: policies never block a job, a query or a pipeline, and a policy asking for ``mode="enforce"`` is refused
with ``422 unsupported``.

Owners, certification and quality exist for several kinds of subject; ``subject_type`` is ``asset``, ``domain``, ``product`` or ``term`` (certification: ``asset``,
``product`` or ``term``; quality: ``asset`` or ``product``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from olcf.sdk._resources import JSON, Body, _Res

SubjectType = Literal["asset", "domain", "product", "term"]
CertSubject = Literal["asset", "product", "term"]
CertState = Literal["none", "draft", "certified", "deprecated"]
Severity = Literal["low", "medium", "high", "critical"]
ChangeType = Literal["certify", "deprecate", "classification_change", "term_link", "ownership_change", "policy_exception"]


def _clean(**fields: Any) -> dict[str, Any]:
    """Drop ``None`` values so the server's own defaults apply."""
    return {k: v for k, v in fields.items() if v is not None}


def _bad_subject(kind: str, allowed: str) -> ValueError:
    return ValueError(f"subject_type must be one of {allowed} (got {kind!r})")


class Governance(_Res):
    """Governance of the asset graph: glossary, domains, data products, owners, classifications, certification, change requests, policies, violations and quality."""

    # ------------------------------------------------------------------------------------------------------------------------ overview
    def summary(self, *, workspace_id: str | None = None) -> JSON:
        """Governance overview: glossary, domains, products, classifications, review queue, violations, certification and coverage percentages."""
        return self._call("GET", "/governance/summary", params={"workspace_id": workspace_id})

    def asset(self, asset_id: str) -> JSON:
        """Everything governance knows about an asset: domain, products, terms, owners (direct and effective), classifications, certification, quality score, violations and open changes."""
        return self._call("GET", "/governance/assets/{id}", path={"id": asset_id})

    def asset_graph(self, asset_id: str) -> JSON:
        """Virtual governance edges around an asset (``GOVERNED_BY`` to domains, products and terms; ``CLASSIFIED_AS`` to classifications); never stored in the lineage graph."""
        return self._call("GET", "/governance/assets/{id}/graph", path={"id": asset_id})

    # ------------------------------------------------------------------------------------------------------------------------ glossary
    def list_terms(self, *, q: str | None = None, status: str | None = None, glossary: str | None = None, domain_id: str | None = None, parent_id: str | None = None,
                   certification: str | None = None, owner: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Glossary terms; ``parent_id="root"`` lists the top level."""
        return self._call("GET", "/governance/glossary/terms", params={"q": q, "status": status, "glossary": glossary, "domain_id": domain_id, "parent_id": parent_id,
                                                                       "certification": certification, "owner": owner, "limit": limit, "cursor": cursor})

    def create_term(self, name: str, *, definition: str | None = None, abbreviation: str | None = None, synonyms: Sequence[str] | None = None, glossary: str | None = None,
                    status: str | None = None, parent_id: str | None = None, domain_id: str | None = None, related: Sequence[str] | None = None,
                    owners: Sequence[Body] | None = None) -> JSON:
        """Create a term (unique per glossary, case-insensitive); ``status="approved"`` needs ``governance.approve``."""
        return self._call("POST", "/governance/glossary/terms", json=_clean(name=name, definition=definition, abbreviation=abbreviation, synonyms=list(synonyms) if synonyms is not None else None,
                                                                            glossary=glossary, status=status, parent_id=parent_id, domain_id=domain_id,
                                                                            related=list(related) if related is not None else None, owners=list(owners) if owners is not None else None))

    def get_term(self, term_id: str) -> JSON:
        """One term with ancestors, children, related terms, owners, certification and link count."""
        return self._call("GET", "/governance/glossary/terms/{id}", path={"id": term_id})

    def update_term(self, term_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """Change a term (``name``, ``definition``, ``abbreviation``, ``synonyms``, ``status``, ``parent_id``, ``domain_id``, ``related``)."""
        return self._call("PATCH", "/governance/glossary/terms/{id}", path={"id": term_id}, if_match=if_match, json=fields)

    def delete_term(self, term_id: str, *, hard: bool = False, if_match: Any = None) -> JSON:
        """Deprecate a term and unlink it everywhere (soft); ``hard=True`` removes the row and its references (needs ``governance.approve``)."""
        return self._call("DELETE", "/governance/glossary/terms/{id}", path={"id": term_id}, params={"hard": hard or None}, if_match=if_match)

    def search_glossary(self, q: str, *, limit: int = 20, include_deprecated: bool = False) -> JSON:
        """Ranked term search (exact name, synonym, abbreviation, prefix, contains, definition); items are ``{term, score, matched_on}``.

        Deprecated terms are left out unless ``include_deprecated`` is true.
        """
        return self._call("GET", "/governance/glossary/search", params={"q": q, "limit": limit, "include_deprecated": include_deprecated or None})

    def term_links(self, term_id: str, *, status: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Assets and products a term is linked to (default statuses ``approved`` and ``pending``)."""
        return self._call("GET", "/governance/glossary/terms/{id}/links", path={"id": term_id}, params={"status": status, "limit": limit, "cursor": cursor})

    def link_term(self, term_id: str, *, asset_id: str | None = None, product_id: str | None = None, comment: str | None = None) -> JSON:
        """Link a term to an asset or a data product (manual links are approved at once)."""
        return self._call("POST", "/governance/glossary/terms/{id}/links", path={"id": term_id}, json=_clean(asset_id=asset_id, product_id=product_id, comment=comment))

    def unlink_term(self, term_id: str, link_id: int) -> JSON:
        """Remove a term link by its integer id."""
        return self._call("DELETE", "/governance/glossary/terms/{id}/links/{link_id}", path={"id": term_id, "link_id": link_id})

    def add_asset_term(self, asset_id: str, term_id: str) -> JSON:
        """Link a term to an asset (the same as :meth:`link_term`, from the asset side)."""
        return self._call("POST", "/governance/assets/{id}/terms", path={"id": asset_id}, json={"term_id": term_id})

    def remove_asset_term(self, asset_id: str, term_id: str) -> JSON:
        """Remove the link between an asset and a term."""
        return self._call("DELETE", "/governance/assets/{id}/terms/{term_id}", path={"id": asset_id, "term_id": term_id})

    def suggest_term_links(self, *, workspace_id: str | None = None) -> JSON:
        """Propose links by exact name, synonym or abbreviation match; they enter the review queue as ``suggested`` (nothing is approved automatically)."""
        return self._call("POST", "/governance/glossary/suggest-links", json=_clean(workspace_id=workspace_id))

    # ------------------------------------------------------------------------------------------------------------------------ domains
    def list_domains(self, *, q: str | None = None, parent_id: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Domains with asset, product and child counts; ``parent_id="root"`` lists the top level."""
        return self._call("GET", "/governance/domains", params={"q": q, "parent_id": parent_id, "limit": limit, "cursor": cursor})

    def create_domain(self, name: str, *, description: str | None = None, parent_id: str | None = None, owners: Sequence[Body] | None = None) -> JSON:
        """Create a domain (optionally below a parent domain)."""
        return self._call("POST", "/governance/domains", json=_clean(name=name, description=description, parent_id=parent_id, owners=list(owners) if owners is not None else None))

    def get_domain(self, domain_id: str) -> JSON:
        """One domain with ancestors, children and owners."""
        return self._call("GET", "/governance/domains/{id}", path={"id": domain_id})

    def update_domain(self, domain_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """Change a domain (``name``, ``description``, ``parent_id``)."""
        return self._call("PATCH", "/governance/domains/{id}", path={"id": domain_id}, if_match=if_match, json=fields)

    def delete_domain(self, domain_id: str, *, if_match: Any = None) -> JSON:
        """Delete a domain (``409`` while it has sub-domains, products, assigned assets or linked terms)."""
        return self._call("DELETE", "/governance/domains/{id}", path={"id": domain_id}, if_match=if_match)

    def domain_assets(self, domain_id: str, *, type: str | None = None, include_descendants: bool = True, limit: int = 50, cursor: str | None = None) -> JSON:  # noqa: A002
        """Assets in a domain: directly, by inheritance from a parent asset, or through a data product."""
        return self._call("GET", "/governance/domains/{id}/assets", path={"id": domain_id},
                          params={"type": type, "include_descendants": include_descendants, "limit": limit, "cursor": cursor})

    def set_asset_domain(self, asset_id: str, domain_id: str | None) -> JSON:
        """Assign an asset to a domain (``None`` clears the assignment)."""
        return self._call("PUT", "/governance/assets/{id}/domain", path={"id": asset_id}, json={"domain_id": domain_id})

    # ------------------------------------------------------------------------------------------------------------------------ data products
    def list_products(self, *, q: str | None = None, status: str | None = None, domain_id: str | None = None, certification: str | None = None, owner: str | None = None,
                      limit: int = 50, cursor: str | None = None) -> JSON:
        """Data products."""
        return self._call("GET", "/governance/products", params={"q": q, "status": status, "domain_id": domain_id, "certification": certification, "owner": owner,
                                                                 "limit": limit, "cursor": cursor})

    def create_product(self, name: str, *, description: str | None = None, domain_id: str | None = None, status: str | None = None, owners: Sequence[Body] | None = None,
                       member_asset_ids: Sequence[str] | None = None, sla: Body | None = None, contract: Body | None = None) -> JSON:
        """Create a data product; ``status="published"`` needs ``governance.approve``.  ``sla`` is ``{freshness_hours, availability}``, ``contract`` ``{columns, expectations}``."""
        return self._call("POST", "/governance/products", json=_clean(name=name, description=description, domain_id=domain_id, status=status,
                                                                      owners=list(owners) if owners is not None else None,
                                                                      member_asset_ids=list(member_asset_ids) if member_asset_ids is not None else None,
                                                                      sla=dict(sla) if sla is not None else None, contract=dict(contract) if contract is not None else None))

    def get_product(self, product_id: str) -> JSON:
        """One product with members (visible only), consumers, quality score, owners, terms and certification."""
        return self._call("GET", "/governance/products/{id}", path={"id": product_id})

    def update_product(self, product_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """Change a product (``name``, ``description``, ``domain_id``, ``status``, ``sla``, ``contract``)."""
        return self._call("PATCH", "/governance/products/{id}", path={"id": product_id}, if_match=if_match, json=fields)

    def delete_product(self, product_id: str, *, if_match: Any = None) -> JSON:
        """Delete a product (``409`` while it is published; deprecate it first)."""
        return self._call("DELETE", "/governance/products/{id}", path={"id": product_id}, if_match=if_match)

    def set_product_members(self, product_id: str, asset_ids: Sequence[str]) -> JSON:
        """Replace the member set of a product."""
        return self._call("PUT", "/governance/products/{id}/members", path={"id": product_id}, json={"asset_ids": list(asset_ids)})

    def add_product_member(self, product_id: str, asset_id: str) -> JSON:
        """Add one asset to a product."""
        return self._call("POST", "/governance/products/{id}/members", path={"id": product_id}, json={"asset_id": asset_id})

    def remove_product_member(self, product_id: str, asset_id: str) -> JSON:
        """Remove one asset from a product."""
        return self._call("DELETE", "/governance/products/{id}/members/{asset_id}", path={"id": product_id, "asset_id": asset_id})

    def product_consumers(self, product_id: str, *, depth: int | None = None, min_confidence: str | None = None, limit: int = 200) -> JSON:
        """Assets and reports downstream of a product's members that are not members, each with the member it reads and the confidence of the path.

        The answer is one bounded document (``items`` with ``asset``, ``via_member``, ``confidence`` and ``hops``, plus ``total`` and ``truncated``), not a paged list;
        raise ``limit`` (up to 500) to see more.
        """
        return self._call("GET", "/governance/products/{id}/consumers", path={"id": product_id}, params={"depth": depth, "min_confidence": min_confidence, "limit": limit})

    # ------------------------------------------------------------------------------------------------------------------------ owners
    def owners(self, subject_type: SubjectType, subject_id: str) -> JSON:
        """Direct and effective owners of an ``asset`` (effective applies inheritance per role), ``domain``, ``product`` or ``term``."""
        if subject_type == "asset":
            return self._call("GET", "/governance/assets/{id}/owners", path={"id": subject_id})
        if subject_type == "domain":
            return self._call("GET", "/governance/domains/{id}/owners", path={"id": subject_id})
        if subject_type == "product":
            return self._call("GET", "/governance/products/{id}/owners", path={"id": subject_id})
        if subject_type == "term":
            return self._call("GET", "/governance/glossary/terms/{id}/owners", path={"id": subject_id})
        raise _bad_subject(subject_type, "asset, domain, product, term")

    def set_owners(self, subject_type: SubjectType, subject_id: str, owners: Sequence[Body]) -> JSON:
        """Replace the direct owners.  An owner is ``{"role": "owner|steward|custodian|contact", "user": "<usr id | e-mail | name>"}`` or ``{"role", "name", "email"}`` for an external contact."""
        body = {"owners": [dict(o) for o in owners]}
        if subject_type == "asset":
            return self._call("PUT", "/governance/assets/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "domain":
            return self._call("PUT", "/governance/domains/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "product":
            return self._call("PUT", "/governance/products/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "term":
            return self._call("PUT", "/governance/glossary/terms/{id}/owners", path={"id": subject_id}, json=body)
        raise _bad_subject(subject_type, "asset, domain, product, term")

    def add_owner(self, subject_type: SubjectType, subject_id: str, owner: Body) -> JSON:
        """Add one owner (``409`` if already assigned)."""
        body = dict(owner)
        if subject_type == "asset":
            return self._call("POST", "/governance/assets/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "domain":
            return self._call("POST", "/governance/domains/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "product":
            return self._call("POST", "/governance/products/{id}/owners", path={"id": subject_id}, json=body)
        if subject_type == "term":
            return self._call("POST", "/governance/glossary/terms/{id}/owners", path={"id": subject_id}, json=body)
        raise _bad_subject(subject_type, "asset, domain, product, term")

    def remove_owner(self, subject_type: SubjectType, subject_id: str, owner_id: int) -> JSON:
        """Remove a direct owner by its integer id."""
        if subject_type == "asset":
            return self._call("DELETE", "/governance/assets/{id}/owners/{owner_id}", path={"id": subject_id, "owner_id": owner_id})
        if subject_type == "domain":
            return self._call("DELETE", "/governance/domains/{id}/owners/{owner_id}", path={"id": subject_id, "owner_id": owner_id})
        if subject_type == "product":
            return self._call("DELETE", "/governance/products/{id}/owners/{owner_id}", path={"id": subject_id, "owner_id": owner_id})
        if subject_type == "term":
            return self._call("DELETE", "/governance/glossary/terms/{id}/owners/{owner_id}", path={"id": subject_id, "owner_id": owner_id})
        raise _bad_subject(subject_type, "asset, domain, product, term")

    # ------------------------------------------------------------------------------------------------------------------------ classifications
    def list_classifications(self) -> JSON:
        """The classification catalog (built-in labels such as PII, PCI, Financial, Credentials, Restricted, and your own); it is short, so there is no paging."""
        return self._call("GET", "/governance/classifications")

    def create_classification(self, name: str, category: str, level: int, *, description: str | None = None, color: str | None = None) -> JSON:
        """Create a label: ``level`` 0..5 (sensitivity), ``color`` ``#rrggbb``."""
        return self._call("POST", "/governance/classifications", json=_clean(name=name, category=category, level=level, description=description, color=color))

    def get_classification(self, classification_id: str) -> JSON:
        """One label."""
        return self._call("GET", "/governance/classifications/{id}", path={"id": classification_id})

    def update_classification(self, classification_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """Change a label (built-in labels allow only ``description`` and ``color``)."""
        return self._call("PATCH", "/governance/classifications/{id}", path={"id": classification_id}, if_match=if_match, json=fields)

    def delete_classification(self, classification_id: str, *, if_match: Any = None) -> JSON:
        """Delete a custom label (``409`` while it has non-rejected assignments)."""
        return self._call("DELETE", "/governance/classifications/{id}", path={"id": classification_id}, if_match=if_match)

    def asset_classifications(self, asset_id: str) -> JSON:
        """Classifications of an asset: ``direct``, ``inherited`` (propagated along the lineage), ``suggestions`` awaiting review and ``contained`` (labelled columns inside it)."""
        return self._call("GET", "/governance/assets/{id}/classifications", path={"id": asset_id})

    def classify_asset(self, asset_id: str, classification: str, *, comment: str | None = None) -> JSON:
        """Assign a label (id or name) to an asset or column.  With four-eyes enabled and without ``governance.approve`` it waits in the review queue as ``suggested``."""
        return self._call("POST", "/governance/assets/{id}/classifications", path={"id": asset_id}, json=_clean(classification=classification, comment=comment))

    def unclassify_asset(self, asset_id: str, classification: str) -> JSON:
        """Remove a label (id or name) from an asset."""
        return self._call("DELETE", "/governance/assets/{id}/classifications/{cls}", path={"id": asset_id, "cls": classification})

    def classification_assets(self, classification_id: str, *, workspace_id: str | None = None, include_inherited: bool = True, type: str | None = None,  # noqa: A002
                              limit: int = 50, cursor: str | None = None) -> JSON:
        """Assets carrying a label (``Which reports show PII?`` starts here), with source, status, confidence and precision."""
        return self._call("GET", "/governance/classifications/{id}/assets", path={"id": classification_id},
                          params={"workspace_id": workspace_id, "include_inherited": include_inherited, "type": type, "limit": limit, "cursor": cursor})

    def classification_exposure(self, classification_id: str, *, depth: int | None = None, min_confidence: str | None = None, include_possible: bool = True,
                                types: str | Sequence[str] | None = None, workspace_id: str | None = None) -> JSON:
        """Where does data labelled X end up?  Reports, visuals, semantic models, products and tables downstream of the labelled assets, with the lineage path and a proven / possible flag."""
        return self._call("GET", "/governance/classifications/{id}/exposure", path={"id": classification_id},
                          params={"depth": depth, "min_confidence": min_confidence, "include_possible": include_possible, "types": (types if isinstance(types, str) else ",".join(types)) if types else None,
                                  "workspace_id": workspace_id})

    def scan_classifications(self, *, workspace_id: str | None = None, scan_id: str | None = None) -> JSON:
        """Ingest the sensitive-data suggestions of the scans now; they enter the review queue (never approved automatically)."""
        return self._call("POST", "/governance/classifications/scan", json=_clean(workspace_id=workspace_id, scan_id=scan_id))

    # ------------------------------------------------------------------------------------------------------------------------ review queue
    def review_queue(self, *, kind: str | None = None, asset_id: str | None = None, workspace_id: str | None = None, classification: str | None = None,
                     term_id: str | None = None, min_confidence: float | None = None, q: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Pending suggestions (classification and term-link), oldest first."""
        return self._call("GET", "/governance/review-queue", params={"kind": kind, "asset_id": asset_id, "workspace_id": workspace_id, "classification": classification,
                                                                     "term_id": term_id, "min_confidence": min_confidence, "q": q, "limit": limit, "cursor": cursor})

    def review_summary(self, *, workspace_id: str | None = None) -> JSON:
        """Counts of pending review items by kind (``classification``, ``term_link``), plus open change requests."""
        return self._call("GET", "/governance/review-queue/summary", params={"workspace_id": workspace_id})

    def approve_review(self, item_id: str, *, comment: str | None = None) -> JSON:
        """Approve a queue item (``"<kind>:<int>"``, kind ``classification`` or ``term_link``)."""
        return self._call("POST", "/governance/review-queue/{item}/approve", path={"item": item_id}, json=_clean(comment=comment))

    def reject_review(self, item_id: str, *, comment: str | None = None) -> JSON:
        """Reject a queue item; a rejected suggestion is not proposed again."""
        return self._call("POST", "/governance/review-queue/{item}/reject", path={"item": item_id}, json=_clean(comment=comment))

    def review_bulk(self, action: Literal["approve", "reject"], ids: Sequence[str], *, comment: str | None = None) -> JSON:
        """Approve or reject up to 500 queue items; the answer lists ``results`` per id with ``ok`` and ``error``."""
        return self._call("POST", "/governance/review-queue/bulk", json=_clean(action=action, ids=list(ids), comment=comment))

    # ------------------------------------------------------------------------------------------------------------------------ certification
    def certification(self, subject_type: CertSubject, subject_id: str) -> JSON:
        """Certification state of an ``asset``, ``product`` or ``term`` (``none``, ``draft``, ``certified``, ``deprecated``) with who certified it and whether its schema changed since."""
        if subject_type == "asset":
            return self._call("GET", "/governance/assets/{id}/certification", path={"id": subject_id})
        if subject_type == "product":
            return self._call("GET", "/governance/products/{id}/certification", path={"id": subject_id})
        if subject_type == "term":
            return self._call("GET", "/governance/glossary/terms/{id}/certification", path={"id": subject_id})
        raise _bad_subject(subject_type, "asset, product, term")

    def set_certification(self, subject_type: CertSubject, subject_id: str, state: CertState, *, comment: str | None = None, if_match: Any = None) -> JSON:
        """Set the certification state; ``certified`` and ``deprecated`` need ``governance.approve``, ``draft`` and ``none`` ``governance.curate``."""
        body = _clean(state=state, comment=comment)
        if subject_type == "asset":
            return self._call("PUT", "/governance/assets/{id}/certification", path={"id": subject_id}, json=body, if_match=if_match)
        if subject_type == "product":
            return self._call("PUT", "/governance/products/{id}/certification", path={"id": subject_id}, json=body, if_match=if_match)
        if subject_type == "term":
            return self._call("PUT", "/governance/glossary/terms/{id}/certification", path={"id": subject_id}, json=body, if_match=if_match)
        raise _bad_subject(subject_type, "asset, product, term")

    def list_certifications(self, *, state: CertState | None = None, subject_type: CertSubject | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """What is certified or deprecated."""
        return self._call("GET", "/governance/certifications", params={"state": state, "subject_type": subject_type, "limit": limit, "cursor": cursor})

    # ------------------------------------------------------------------------------------------------------------------------ change requests
    def list_changes(self, *, status: str | None = None, type: str | None = None, subject_type: str | None = None, subject_id: str | None = None,  # noqa: A002
                     requester: str | None = None, reviewer: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Change requests (``requester="me"``, ``reviewer="me"`` narrow to yours)."""
        return self._call("GET", "/governance/changes", params={"status": status, "type": type, "subject_type": subject_type, "subject_id": subject_id,
                                                               "requester": requester, "reviewer": reviewer, "limit": limit, "cursor": cursor})

    def request_change(self, type: ChangeType, subject_type: str, subject_id: str, payload: Mapping[str, Any] | None = None, *,  # noqa: A002
                       reviewers: Sequence[str] | None = None, comment: str | None = None) -> JSON:
        """Open a change request (certify, deprecate, classification_change, term_link, ownership_change, policy_exception); it is applied when a reviewer approves it."""
        return self._call("POST", "/governance/changes", json=_clean(type=type, subject_type=subject_type, subject_id=subject_id, payload=dict(payload or {}),
                                                                    reviewers=list(reviewers) if reviewers is not None else None, comment=comment))

    def get_change(self, change_id: str) -> JSON:
        """One change request with its comments."""
        return self._call("GET", "/governance/changes/{id}", path={"id": change_id})

    def comment_change(self, change_id: str, text: str) -> JSON:
        """Add a comment to a change request."""
        return self._call("POST", "/governance/changes/{id}/comments", path={"id": change_id}, json={"text": text})

    def approve_change(self, change_id: str, *, comment: str | None = None) -> JSON:
        """Approve and apply a change (refused for the requester unless ``governance.allow_self_approval``)."""
        return self._call("POST", "/governance/changes/{id}/approve", path={"id": change_id}, json=_clean(comment=comment))

    def reject_change(self, change_id: str, comment: str) -> JSON:
        """Reject a change; the comment is required."""
        return self._call("POST", "/governance/changes/{id}/reject", path={"id": change_id}, json={"comment": comment})

    def withdraw_change(self, change_id: str) -> JSON:
        """Withdraw your own pending change."""
        return self._call("POST", "/governance/changes/{id}/withdraw", path={"id": change_id})

    def apply_change(self, change_id: str) -> JSON:
        """Re-apply an approved change whose application failed (idempotent)."""
        return self._call("POST", "/governance/changes/{id}/apply", path={"id": change_id})

    # ------------------------------------------------------------------------------------------------------------------------ policies and violations
    def rule_types(self) -> JSON:
        """The closed set of policy rule types with the documentation of their parameters."""
        return self._call("GET", "/governance/policies/rule-types")

    def list_policies(self, *, rule_type: str | None = None, enabled: bool | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Policies with their open violation counts and last evaluation time."""
        return self._call("GET", "/governance/policies", params={"rule_type": rule_type, "enabled": enabled, "limit": limit, "cursor": cursor})

    def create_policy(self, name: str, rule_type: str, params: Mapping[str, Any], *, description: str | None = None, severity: Severity | None = None, mode: str | None = None,
                      enabled: bool | None = None) -> JSON:
        """Create a policy (observe mode: it reports violations, it never blocks anything; ``mode="enforce"`` is refused with ``422 unsupported``)."""
        return self._call("POST", "/governance/policies", json=_clean(name=name, rule_type=rule_type, params=dict(params), description=description, severity=severity, mode=mode,
                                                                     enabled=enabled))

    def get_policy(self, policy_id: str) -> JSON:
        """One policy."""
        return self._call("GET", "/governance/policies/{id}", path={"id": policy_id})

    def update_policy(self, policy_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """Change a policy (``name``, ``description``, ``params``, ``severity``, ``enabled``)."""
        return self._call("PATCH", "/governance/policies/{id}", path={"id": policy_id}, if_match=if_match, json=fields)

    def delete_policy(self, policy_id: str, *, if_match: Any = None) -> JSON:
        """Delete a policy and its violations."""
        return self._call("DELETE", "/governance/policies/{id}", path={"id": policy_id}, if_match=if_match)

    def evaluate_policy(self, policy_id: str) -> JSON:
        """Evaluate one policy now; the answer counts violations opened, updated, resolved, reopened and unknown."""
        return self._call("POST", "/governance/policies/{id}/evaluate", path={"id": policy_id})

    def evaluate_policies(self) -> JSON:
        """Evaluate every enabled policy now."""
        return self._call("POST", "/governance/policies/evaluate")

    def preview_policy(self, rule_type: str, params: Mapping[str, Any], *, severity: Severity | None = None, limit: int | None = None) -> JSON:
        """What a policy *would* report, without storing anything."""
        return self._call("POST", "/governance/policies/preview", json=_clean(rule_type=rule_type, params=dict(params), severity=severity, limit=limit))

    def list_violations(self, *, policy_id: str | None = None, asset_id: str | None = None, severity: Severity | None = None, status: str | None = None,
                        rule_type: str | None = None, workspace_id: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Violations (default: open and acknowledged; ``status="any"`` for all), on assets you can see."""
        return self._call("GET", "/governance/violations", params={"policy_id": policy_id, "asset_id": asset_id, "severity": severity, "status": status, "rule_type": rule_type,
                                                                  "workspace_id": workspace_id, "limit": limit, "cursor": cursor})

    def get_violation(self, violation_id: str) -> JSON:
        """One violation with its policy, asset and evidence."""
        return self._call("GET", "/governance/violations/{id}", path={"id": violation_id})

    def acknowledge_violation(self, violation_id: str, *, comment: str | None = None) -> JSON:
        """Acknowledge a violation: still reported, but no longer open."""
        return self._call("POST", "/governance/violations/{id}/acknowledge", path={"id": violation_id}, json=_clean(comment=comment))

    def reopen_violation(self, violation_id: str) -> JSON:
        """Reopen an acknowledged or excepted violation."""
        return self._call("POST", "/governance/violations/{id}/reopen", path={"id": violation_id}, json={})

    # ------------------------------------------------------------------------------------------------------------------------ quality
    def quality(self, subject_type: Literal["asset", "product"], subject_id: str) -> JSON:
        """Quality score 0..100 of an ``asset`` (documentation, ownership, classification, freshness, certification, lineage, violations, schema stability) or a ``product`` (members)."""
        if subject_type == "asset":
            return self._call("GET", "/governance/assets/{id}/quality", path={"id": subject_id})
        if subject_type == "product":
            return self._call("GET", "/governance/products/{id}/quality", path={"id": subject_id})
        raise _bad_subject(subject_type, "asset, product")


__all__ = ["Governance"]
