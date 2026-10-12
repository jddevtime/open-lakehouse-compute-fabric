# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of governance (mounted under ``/v1/governance``): glossary, domains and data products, owners, classifications and the review queue, certification and change
requests, policies and violations, quality, and the overview / per-asset views. The contract is ``docs/dev/GOVERNANCE_API.md``; the rules live in the managers, these functions only
map HTTP to them.

Permissions (checked by the managers): ``intelligence.read`` reads, ``governance.curate`` edits, ``governance.approve`` approves, administers policies and certifies. An asset the
caller cannot see answers ``404`` like one that does not exist. Mutable single resources return an ``ETag`` and honour ``If-Match``. If the governance storage is missing every route
answers ``503 unavailable`` and the rest of the platform keeps working.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.exc import DBAPIError

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, etag_header
from olcf.intelligence.governance import models as m

log = logging.getLogger("olcf.intelligence.governance")

def _gov(ctx: Any) -> Any:
    svc = ctx.service("intelligence")
    return svc.governance


def _storage_guard(ctx: Ctx) -> Iterator[None]:
    """Turn a failing governance store (missing table, locked or unreachable database) into ``503 unavailable`` instead of an unhandled ``500``."""
    gov = _gov(ctx)
    try:
        yield
    except DBAPIError as exc:
        log.warning("governance storage error: %s", str(exc.orig or exc)[:300])
        raise gov.guard(exc) from exc


router = APIRouter(prefix="/governance", tags=["governance"], dependencies=[Depends(_storage_guard)])


def _etagged(out: dict[str, Any], response: Response) -> dict[str, Any]:
    if out.get("etag"):
        response.headers["ETag"] = etag_header(out["etag"])
    return out


def _page(items: list[Any], nxt: str | None) -> dict[str, Any]:
    return {"items": items, "next_cursor": nxt}


# ============================================================================================================================ glossary
@router.get("/glossary/terms")
def list_terms(principal: Authed, ctx: Ctx, page: Pagination, q: str | None = Query(None, max_length=200), status: str | None = Query(None, pattern="^(draft|approved|deprecated)$"),
               glossary: str | None = Query(None, max_length=64), domain_id: str | None = None, parent_id: str | None = None, certification: str | None = None,
               owner: str | None = None) -> dict[str, Any]:
    """Glossary terms (`parent_id=root` = top level), in creation order."""
    items, nxt = _gov(ctx).glossary.list(principal, q=q, status=status, glossary=glossary, domain_id=domain_id, parent_id=parent_id, certification=certification, owner=owner,
                                         limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/glossary/terms", status_code=201)
def create_term(body: m.TermCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a term (`governance.curate` org-wide; `status: approved` needs `governance.approve`)."""
    return _etagged(_gov(ctx).glossary.create(principal, body.sent()), response)


@router.get("/glossary/search")
def search_terms(principal: Authed, ctx: Ctx, q: str = Query(..., min_length=1, max_length=200), limit: int = Query(25, ge=1, le=100), include_deprecated: bool = False) -> dict[str, Any]:
    """Ranked search over names, synonyms, abbreviations and definitions."""
    return {"items": _gov(ctx).glossary.search(principal, q, limit=limit, include_deprecated=include_deprecated)}


@router.post("/glossary/suggest-links")
def suggest_links(principal: Authed, ctx: Ctx, body: m.SuggestLinks | None = None) -> dict[str, Any]:
    """Propose term links by exact name / synonym / abbreviation match; they wait in the review queue."""
    return _gov(ctx).glossary.suggest(principal, body.workspace_id if body else None)


@router.get("/glossary/terms/{term_id}")
def get_term(term_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One term with ancestors, children, related terms, owners and certification."""
    return _etagged(_gov(ctx).glossary.get(principal, term_id), response)


@router.patch("/glossary/terms/{term_id}")
def update_term(term_id: str, body: m.TermUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a term (`If-Match`; approving needs `governance.approve`)."""
    return _etagged(_gov(ctx).glossary.update(principal, term_id, body.sent(), if_match), response)


@router.delete("/glossary/terms/{term_id}")
def delete_term(term_id: str, principal: Authed, ctx: Ctx, hard: bool = False, if_match: IfMatch = None) -> dict[str, Any]:
    """Soft delete (deprecate and unlink) or, with `hard=true` (`governance.approve`), remove for good."""
    return _gov(ctx).glossary.delete(principal, term_id, hard=hard, if_match=if_match)


@router.get("/glossary/terms/{term_id}/links")
def term_links(term_id: str, principal: Authed, ctx: Ctx, page: Pagination, status: str | None = None) -> dict[str, Any]:
    """Assets and data products linked to a term (hidden assets are left out)."""
    items, nxt = _gov(ctx).glossary.links_of_term(principal, term_id, status=status, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/glossary/terms/{term_id}/links", status_code=201)
def link_term(term_id: str, body: m.TermLinkCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Link a term to an asset (`asset_id`) or a data product (`product_id`)."""
    return _gov(ctx).glossary.link(principal, term_id, asset_id=body.asset_id, product_id=body.product_id, comment=body.comment)


@router.delete("/glossary/terms/{term_id}/links/{link_id}")
def unlink_term(term_id: str, link_id: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Remove a link (kept as history)."""
    return _gov(ctx).glossary.unlink(principal, term_id, link_id)


@router.post("/assets/{asset_id}/terms", status_code=201)
def link_asset_term(asset_id: str, body: m.AssetTermLink, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Link a glossary term to this asset."""
    return _gov(ctx).glossary.link(principal, body.term_id, asset_id=asset_id, comment=body.comment)


@router.delete("/assets/{asset_id}/terms/{term_id}")
def unlink_asset_term(asset_id: str, term_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Unlink a glossary term from this asset."""
    gov = _gov(ctx)
    row = gov.glossary.asset(principal, asset_id)
    return gov.glossary.unlink_from_subject(principal, term_id, "asset", row["id"])


# ============================================================================================================================ domains and products
@router.get("/domains")
def list_domains(principal: Authed, ctx: Ctx, page: Pagination, q: str | None = Query(None, max_length=200), parent_id: str | None = None) -> dict[str, Any]:
    """Domains (`parent_id=root` = top level) with asset, product and child counts."""
    items, nxt = _gov(ctx).domains.list(principal, q=q, parent_id=parent_id, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/domains", status_code=201)
def create_domain(body: m.DomainCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a domain."""
    return _etagged(_gov(ctx).domains.create(principal, body.sent()), response)


@router.get("/domains/{domain_id}")
def get_domain(domain_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One domain with ancestors, children and owners."""
    return _etagged(_gov(ctx).domains.get(principal, domain_id), response)


@router.patch("/domains/{domain_id}")
def update_domain(domain_id: str, body: m.DomainUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a domain (`If-Match`)."""
    return _etagged(_gov(ctx).domains.update(principal, domain_id, body.sent(), if_match), response)


@router.delete("/domains/{domain_id}")
def delete_domain(domain_id: str, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    """Delete a domain (`409` while it has sub-domains, products, assets or terms)."""
    return _gov(ctx).domains.delete(principal, domain_id, if_match)


@router.get("/domains/{domain_id}/assets")
def domain_assets(domain_id: str, principal: Authed, ctx: Ctx, page: Pagination, type: str | None = None, include_descendants: bool = True) -> dict[str, Any]:
    """Visible assets in the domain: direct, inherited from a parent asset, or through a data product."""
    items, nxt = _gov(ctx).domains.assets_of(principal, domain_id, type=type, include_descendants=include_descendants, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.put("/assets/{asset_id}/domain")
def set_asset_domain(asset_id: str, body: m.AssetDomain, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Assign (or with `null` clear) the domain of an asset."""
    return _gov(ctx).domains.set_asset_domain(principal, asset_id, body.domain_id)


@router.get("/products")
def list_products(principal: Authed, ctx: Ctx, page: Pagination, q: str | None = Query(None, max_length=200), status: str | None = Query(None, pattern="^(draft|published|deprecated)$"),
                  domain_id: str | None = None, certification: str | None = None, owner: str | None = None) -> dict[str, Any]:
    """Data products."""
    items, nxt = _gov(ctx).products.list(principal, q=q, status=status, domain_id=domain_id, certification=certification, owner=owner, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/products", status_code=201)
def create_product(body: m.ProductCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a data product (`status: published` needs `governance.approve`)."""
    return _etagged(_gov(ctx).products.create(principal, body.sent()), response)


@router.get("/products/{product_id}")
def get_product(product_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One data product with visible members, consumers, quality, owners, terms and certification."""
    return _etagged(_gov(ctx).products.get(principal, product_id), response)


@router.patch("/products/{product_id}")
def update_product(product_id: str, body: m.ProductUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a data product (`If-Match`)."""
    return _etagged(_gov(ctx).products.update(principal, product_id, body.sent(), if_match), response)


@router.delete("/products/{product_id}")
def delete_product(product_id: str, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    """Delete a data product (`409` while published)."""
    return _gov(ctx).products.delete(principal, product_id, if_match)


@router.put("/products/{product_id}/members")
def set_members(product_id: str, body: m.ProductMembers, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Replace the member set (`{"asset_ids": [...]}`)."""
    return _gov(ctx).products.set_members(principal, product_id, body.asset_ids)


@router.post("/products/{product_id}/members", status_code=201)
def add_member(product_id: str, body: m.ProductMember, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Add one member asset."""
    return _gov(ctx).products.add_member(principal, product_id, body.asset_id)


@router.delete("/products/{product_id}/members/{asset_id}")
def remove_member(product_id: str, asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Remove one member asset."""
    return _gov(ctx).products.remove_member(principal, product_id, asset_id)


@router.get("/products/{product_id}/consumers")
def product_consumers(product_id: str, principal: Authed, ctx: Ctx, depth: int = Query(3, ge=1, le=6), min_confidence: str | None = Query(None, pattern="^(verified|derived|inferred|unknown)$"),
                      limit: int = Query(200, ge=1, le=500)) -> dict[str, Any]:
    """Assets downstream of the members that are not members, derived from lineage."""
    return _gov(ctx).products.consumers(principal, product_id, depth=depth, min_confidence=min_confidence, limit=limit)


@router.get("/products/{product_id}/quality")
def product_quality(product_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Quality of a data product: its own components blended with the mean quality of its members."""
    return _gov(ctx).quality.product(principal, product_id)


# ============================================================================================================================ owners
def _owner_routes(prefix: str, subject_type: str) -> None:
    @router.get(f"/{prefix}/{{subject_id}}/owners", name=f"{subject_type}_owners")
    def get_owners(subject_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
        """Direct and effective owners / stewards."""
        return _gov(ctx).owners.owners_of_subject(principal, subject_type, subject_id)

    @router.put(f"/{prefix}/{{subject_id}}/owners", name=f"{subject_type}_owners_replace")
    def put_owners(subject_id: str, body: m.OwnersReplace, principal: Authed, ctx: Ctx) -> dict[str, Any]:
        """Replace the direct owners (`{"owners": [...]}`)."""
        return _gov(ctx).owners.replace(principal, subject_type, subject_id, body.sent()["owners"])

    @router.post(f"/{prefix}/{{subject_id}}/owners", status_code=201, name=f"{subject_type}_owners_add")
    def post_owner(subject_id: str, body: m.OwnerSpec, principal: Authed, ctx: Ctx) -> dict[str, Any]:
        """Add one owner (`409` if already assigned)."""
        return _gov(ctx).owners.add(principal, subject_type, subject_id, body.sent())

    @router.delete(f"/{prefix}/{{subject_id}}/owners/{{owner_id}}", name=f"{subject_type}_owners_remove")
    def delete_owner(subject_id: str, owner_id: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
        """Remove one direct owner."""
        gov = _gov(ctx)
        gov.owners.remove(principal, subject_type, subject_id, owner_id)
        return gov.owners.owners_of_subject(principal, subject_type, subject_id)


for _prefix, _stype in (("assets", "asset"), ("domains", "domain"), ("products", "product"), ("glossary/terms", "term")):
    _owner_routes(_prefix, _stype)


# ============================================================================================================================ classifications
@router.get("/classifications")
def list_classifications(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The classification catalog (built-in and custom labels)."""
    return {"items": _gov(ctx).classifications.list(principal)}


@router.post("/classifications", status_code=201)
def create_classification(body: m.ClassificationCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a custom label."""
    return _etagged(_gov(ctx).classifications.create(principal, body.sent()), response)


@router.post("/classifications/scan")
def scan_classifications(principal: Authed, ctx: Ctx, body: m.ClassificationScan | None = None) -> dict[str, Any]:
    """Ingest the classification suggestions found by discovery into the review queue (nothing is approved)."""
    return _gov(ctx).classifications.scan(principal, body.workspace_id if body else None, body.scan_id if body else None)


@router.get("/classifications/{class_id}")
def get_classification(class_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One label."""
    return _etagged(_gov(ctx).classifications.get(principal, class_id), response)


@router.patch("/classifications/{class_id}")
def update_classification(class_id: str, body: m.ClassificationUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a label (built-in labels allow only description and color)."""
    return _etagged(_gov(ctx).classifications.update(principal, class_id, body.sent(), if_match), response)


@router.delete("/classifications/{class_id}")
def delete_classification(class_id: str, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    """Delete a custom label (`governance.approve`; `409` while it is assigned)."""
    return _gov(ctx).classifications.delete(principal, class_id, if_match)


@router.get("/classifications/{class_id}/assets")
def classification_assets(class_id: str, principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, include_inherited: bool = True,
                          type: str | None = None) -> dict[str, Any]:
    """Assets carrying a label directly or (default) by lineage inheritance."""
    items, nxt = _gov(ctx).classifications.assets_of(principal, class_id, workspace_id=workspace_id, include_inherited=include_inherited, type=type, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.get("/classifications/{class_id}/exposure")
def classification_exposure(class_id: str, principal: Authed, ctx: Ctx, depth: int | None = Query(None, ge=1, le=10),
                            min_confidence: str | None = Query(None, pattern="^(verified|derived|inferred|unknown)$"), include_possible: bool = True, types: str | None = None,
                            workspace_id: str | None = None) -> dict[str, Any]:
    """Where does data labelled X end up (reports, visuals, semantic models, data products ...), with the lineage path as evidence."""
    return _gov(ctx).classifications.exposure(principal, class_id, depth=depth, min_confidence=min_confidence, include_possible=include_possible, types=types, workspace_id=workspace_id)


@router.get("/assets/{asset_id}/classifications")
def asset_classifications(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Direct, inherited (computed), suggested and rejected labels of an asset and the labels contained in its columns."""
    return _gov(ctx).classifications.for_asset(principal, asset_id)


@router.post("/assets/{asset_id}/classifications", status_code=201)
def assign_classification(asset_id: str, body: m.ClassificationAssign, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Assign a label (`approved`, or `suggested` while four-eyes approval is on and the caller cannot approve)."""
    return _gov(ctx).classifications.assign(principal, asset_id, body.classification, comment=body.comment)


@router.delete("/assets/{asset_id}/classifications/{class_ref}")
def remove_classification(asset_id: str, class_ref: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Remove a label from an asset."""
    return _gov(ctx).classifications.remove(principal, asset_id, class_ref)


@router.get("/review-queue")
def review_queue(principal: Authed, ctx: Ctx, page: Pagination, kind: str | None = Query(None, pattern="^(classification|term_link)$"), asset_id: str | None = None,
                 workspace_id: str | None = None, classification: str | None = None, term_id: str | None = None, min_confidence: float | None = Query(None, ge=0, le=1),
                 q: str | None = Query(None, max_length=200)) -> dict[str, Any]:
    """Pending classification and glossary-link suggestions, oldest first."""
    items, nxt = _gov(ctx).classifications.queue(principal, kind=kind, asset_id=asset_id, workspace_id=workspace_id, classification=classification, term_id=term_id,
                                                 min_confidence=min_confidence, q=q, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.get("/review-queue/summary")
def review_queue_summary(principal: Authed, ctx: Ctx, workspace_id: str | None = None) -> dict[str, Any]:
    """Counts of pending items by kind and pending change requests."""
    return _gov(ctx).classifications.queue_counts(principal, workspace_id)


@router.post("/review-queue/bulk")
def review_bulk(body: m.ReviewBulk, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Approve or reject up to 500 items; each is decided on its own."""
    return _gov(ctx).classifications.bulk(principal, body.action, body.ids, body.comment)


@router.post("/review-queue/{item_id}/approve")
def review_approve(item_id: str, principal: Authed, ctx: Ctx, body: m.ReviewDecision | None = None) -> dict[str, Any]:
    """Approve one suggestion."""
    return _gov(ctx).classifications.review(principal, item_id, "approve", body.comment if body else None)


@router.post("/review-queue/{item_id}/reject")
def review_reject(item_id: str, principal: Authed, ctx: Ctx, body: m.ReviewDecision | None = None) -> dict[str, Any]:
    """Reject one suggestion (it is not proposed again)."""
    return _gov(ctx).classifications.review(principal, item_id, "reject", body.comment if body else None)


# ============================================================================================================================ certification and changes
def _cert_routes(prefix: str, subject_type: str) -> None:
    @router.get(f"/{prefix}/{{subject_id}}/certification", name=f"{subject_type}_certification")
    def get_cert(subject_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
        """Certification state."""
        return _etagged(_gov(ctx).certification.get(principal, subject_type, subject_id), response)

    @router.put(f"/{prefix}/{{subject_id}}/certification", name=f"{subject_type}_certification_set")
    def put_cert(subject_id: str, body: m.CertificationPut, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
        """Set the certification state (`draft` / `none`: curate; `certified` / `deprecated`: approve)."""
        return _etagged(_gov(ctx).certification.put(principal, subject_type, subject_id, body.state, body.comment, if_match), response)


for _prefix, _stype in (("assets", "asset"), ("products", "product"), ("glossary/terms", "term")):
    _cert_routes(_prefix, _stype)


@router.get("/certifications")
def list_certifications(principal: Authed, ctx: Ctx, page: Pagination, state: str | None = None, subject_type: str | None = None) -> dict[str, Any]:
    """What is certified / deprecated (default) or in another state."""
    items, nxt = _gov(ctx).certification.list(principal, state=state, subject_type=subject_type, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.get("/changes")
def list_changes(principal: Authed, ctx: Ctx, page: Pagination, status: str | None = None, type: str | None = None, subject_type: str | None = None, subject_id: str | None = None,
                 requester: str | None = None, reviewer: str | None = None) -> dict[str, Any]:
    """Change requests the caller can see (`requester=me`, `reviewer=me`)."""
    items, nxt = _gov(ctx).workflow.list(principal, status=status, type=type, subject_type=subject_type, subject_id=subject_id, requester=requester, reviewer=reviewer,
                                         limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/changes", status_code=201)
def create_change(body: m.ChangeCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Open a change request (certify, deprecate, classification_change, term_link, ownership_change, policy_exception)."""
    return _etagged(_gov(ctx).workflow.create(principal, body.sent()), response)


@router.get("/changes/{change_id}")
def get_change(change_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One change request with its comments."""
    return _etagged(_gov(ctx).workflow.get(principal, change_id), response)


@router.post("/changes/{change_id}/comments")
def comment_change(change_id: str, body: m.ChangeComment, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Add a comment."""
    return _gov(ctx).workflow.comment(principal, change_id, body.text)


@router.post("/changes/{change_id}/approve")
def approve_change(change_id: str, principal: Authed, ctx: Ctx, body: m.ReviewDecision | None = None) -> dict[str, Any]:
    """Approve and apply (not your own request unless `governance.allow_self_approval`)."""
    return _gov(ctx).workflow.approve(principal, change_id, body.comment if body else None)


@router.post("/changes/{change_id}/reject")
def reject_change(change_id: str, body: m.ChangeReject, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Reject; a comment is required."""
    return _gov(ctx).workflow.reject(principal, change_id, body.comment)


@router.post("/changes/{change_id}/withdraw")
def withdraw_change(change_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The requester withdraws a pending request."""
    return _gov(ctx).workflow.withdraw(principal, change_id)


@router.post("/changes/{change_id}/apply")
def apply_change(change_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Re-apply an approved change whose application failed (idempotent)."""
    return _gov(ctx).workflow.apply(principal, change_id)


# ============================================================================================================================ policies and violations
@router.get("/policies/rule-types")
def rule_types(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The closed set of rule types with parameter documentation."""
    gov = _gov(ctx)
    gov.policies.org_reader(principal)
    return {"items": gov.policies.rule_types()}


@router.post("/policies/preview")
def preview_policy(body: m.PolicyPreview, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """What a policy would report, without storing anything."""
    return _gov(ctx).policies.preview(principal, body.sent())


@router.post("/policies/evaluate")
def evaluate_all(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Evaluate every enabled policy now."""
    return {"results": _gov(ctx).policies.evaluate_all(principal)}


@router.get("/policies")
def list_policies(principal: Authed, ctx: Ctx, page: Pagination, rule_type: str | None = None, enabled: bool | None = None) -> dict[str, Any]:
    """Policies with their open violation counts."""
    items, nxt = _gov(ctx).policies.list(principal, rule_type=rule_type, enabled=enabled, limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.post("/policies", status_code=201)
def create_policy(body: m.PolicyCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a policy (observe mode only; `mode: enforce` is refused)."""
    return _etagged(_gov(ctx).policies.create(principal, body.sent()), response)


@router.get("/policies/{policy_id}")
def get_policy(policy_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One policy."""
    return _etagged(_gov(ctx).policies.get(principal, policy_id), response)


@router.patch("/policies/{policy_id}")
def update_policy(policy_id: str, body: m.PolicyUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a policy (`If-Match`)."""
    return _etagged(_gov(ctx).policies.update(principal, policy_id, body.sent(), if_match), response)


@router.delete("/policies/{policy_id}")
def delete_policy(policy_id: str, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    """Delete a policy and its violations."""
    return _gov(ctx).policies.delete(principal, policy_id, if_match)


@router.post("/policies/{policy_id}/evaluate")
def evaluate_policy(policy_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Evaluate one policy now (idempotent)."""
    return _gov(ctx).policies.evaluate(principal, policy_id)


@router.get("/violations")
def list_violations(principal: Authed, ctx: Ctx, page: Pagination, policy_id: str | None = None, asset_id: str | None = None, severity: str | None = None,
                    status: str | None = None, rule_type: str | None = None, workspace_id: str | None = None) -> dict[str, Any]:
    """Violations of visible assets (default `open,acknowledged`; `status=any` for all)."""
    items, nxt = _gov(ctx).policies.violations(principal, policy_id=policy_id, asset_id=asset_id, severity=severity, status=status, rule_type=rule_type, workspace_id=workspace_id,
                                               limit=page.limit, cursor=page.cursor)
    return _page(items, nxt)


@router.get("/violations/{violation_id}")
def get_violation(violation_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One violation with its evidence."""
    return _etagged(_gov(ctx).policies.violation(principal, violation_id), response)


@router.post("/violations/{violation_id}/acknowledge")
def acknowledge_violation(violation_id: str, principal: Authed, ctx: Ctx, response: Response, body: m.ReviewDecision | None = None) -> dict[str, Any]:
    """Mark a violation as seen (it stays reported)."""
    return _etagged(_gov(ctx).policies.acknowledge(principal, violation_id, body.comment if body else None), response)


@router.post("/violations/{violation_id}/reopen")
def reopen_violation(violation_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Reopen an acknowledged or excepted violation."""
    return _etagged(_gov(ctx).policies.reopen(principal, violation_id), response)


# ============================================================================================================================ quality, overview, per-asset
@router.get("/assets/{asset_id}/quality")
def asset_quality(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Quality score of an asset with its explainable breakdown and roll-up."""
    return _gov(ctx).quality.for_asset(principal, asset_id)


@router.get("/summary")
def summary(principal: Authed, ctx: Ctx, workspace_id: str | None = None) -> dict[str, Any]:
    """Governance overview: counts, review queue, violations, certification and coverage of what the caller can see."""
    return _gov(ctx).summary(principal, workspace_id)


@router.get("/assets/{asset_id}")
def asset_detail(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Everything governance knows about one asset."""
    return _gov(ctx).asset_detail(principal, asset_id)


@router.get("/assets/{asset_id}/graph")
def asset_graph(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Virtual `GOVERNED_BY` / `CLASSIFIED_AS` edges of an asset (computed from governance tables, never stored in the lineage graph)."""
    return _gov(ctx).asset_graph(principal, asset_id)
