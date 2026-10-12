# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Request bodies of the governance REST API (pydantic v2).

Every write route takes one of these models: unknown fields are refused (``extra="forbid"``), types, lengths and enumerations are checked by FastAPI (a bad request is a
``422 validation_failed`` that names the field) and OpenAPI documents the fields for generated clients. The managers still validate the *semantics* (does the parent exist, may
this caller do it, is the name taken) and word those errors, so the handlers pass them :func:`dump` - only the keys the caller actually sent - which keeps "absent" and "explicit
null" apart (``PATCH {"parent_id": null}`` clears the parent, ``PATCH {}`` changes nothing).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from olcf.core.models import dump

Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
ShortName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]
Ident = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
Comment = Annotated[str, StringConstraints(max_length=2000)]
Markdown = Annotated[str, StringConstraints(max_length=20000)]
Description = Annotated[str, StringConstraints(max_length=8000)]
Color = Annotated[str, StringConstraints(pattern=r"^#[0-9a-fA-F]{6}$")]

OwnerRole = Literal["owner", "steward", "custodian", "contact"]
TermStatus = Literal["draft", "approved", "deprecated"]
ProductStatus = Literal["draft", "published", "deprecated"]
CertState = Literal["none", "draft", "certified", "deprecated"]
Severity = Literal["low", "medium", "high", "critical"]
RuleType = Literal["classification_flow", "required_owner", "required_description", "required_classification", "certified_sources_only", "freshness", "quality_min"]
ChangeType = Literal["certify", "deprecate", "classification_change", "term_link", "ownership_change", "policy_exception"]
ChangeSubject = Literal["asset", "product", "term", "domain", "violation"]


class Body(BaseModel):
    """Base of every governance request body: unknown fields are an error."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    def sent(self) -> dict[str, Any]:
        """Only the keys the caller sent (nested models too), as plain data for the managers."""
        return dump(self)


# ============================================================================================================================ owners
class OwnerSpec(Body):
    """One owner or steward: an OLCF user (``user``: id, name or e-mail) **or** an external contact (``name`` and/or ``email``)."""

    role: OwnerRole = Field("owner", description="owner, steward, custodian or contact")
    user: Annotated[str, StringConstraints(min_length=1, max_length=255)] | None = Field(None, description="an OLCF user: id, user name or e-mail")
    name: Annotated[str, StringConstraints(max_length=255)] | None = Field(None, description="display name of an external contact")
    email: Annotated[str, StringConstraints(max_length=255)] | None = Field(None, description="e-mail of an external contact")
    notes: Annotated[str, StringConstraints(max_length=500)] | None = None


class OwnersReplace(Body):
    """Body of ``PUT .../owners``: the complete direct owner list (an empty list removes them all)."""

    owners: list[OwnerSpec] = Field(max_length=50)


# ============================================================================================================================ glossary
class TermCreate(Body):
    """Body of ``POST /governance/glossary/terms``."""

    name: Name
    definition: Markdown | None = None
    abbreviation: ShortName | None = None
    synonyms: list[Annotated[str, StringConstraints(max_length=255)]] | None = Field(None, max_length=50)
    glossary: ShortName | None = Field(None, description="namespace of the term (default 'default')")
    status: TermStatus = "draft"
    parent_id: Ident | None = None
    domain_id: Ident | None = None
    related: list[Ident] | None = Field(None, max_length=100)
    owners: list[OwnerSpec] | None = Field(None, max_length=50)


class TermUpdate(Body):
    """Body of ``PATCH /governance/glossary/terms/{id}``: only the fields present change."""

    name: Name | None = None
    definition: Markdown | None = None
    abbreviation: ShortName | None = None
    synonyms: list[Annotated[str, StringConstraints(max_length=255)]] | None = Field(None, max_length=50)
    status: TermStatus | None = None
    parent_id: Ident | None = None
    domain_id: Ident | None = None
    related: list[Ident] | None = Field(None, max_length=100)


class SuggestLinks(Body):
    """Body of ``POST /governance/glossary/suggest-links``."""

    workspace_id: Ident | None = None


class TermLinkCreate(Body):
    """Body of ``POST /governance/glossary/terms/{id}/links``: exactly one of ``asset_id`` and ``product_id``."""

    asset_id: Ident | None = None
    product_id: Ident | None = None
    comment: Comment | None = None


class AssetTermLink(Body):
    """Body of ``POST /governance/assets/{id}/terms``."""

    term_id: Ident
    comment: Comment | None = None


# ============================================================================================================================ domains and products
class DomainCreate(Body):
    """Body of ``POST /governance/domains``."""

    name: Name
    description: Description | None = None
    parent_id: Ident | None = None
    owners: list[OwnerSpec] | None = Field(None, max_length=50)


class DomainUpdate(Body):
    """Body of ``PATCH /governance/domains/{id}``."""

    name: Name | None = None
    description: Description | None = None
    parent_id: Ident | None = None


class AssetDomain(Body):
    """Body of ``PUT /governance/assets/{id}/domain``: ``domain_id`` is required; ``null`` clears the assignment."""

    domain_id: Ident | None


class ProductSla(Body):
    """Service levels a data product promises."""

    freshness_hours: float | None = Field(None, gt=0, le=87600)
    availability: Annotated[str, StringConstraints(max_length=500)] | None = None


class ContractColumn(Body):
    """One column of a data-contract."""

    name: Name
    type: Annotated[str, StringConstraints(max_length=2000)] | None = None
    description: Annotated[str, StringConstraints(max_length=2000)] | None = None
    nullable: bool | None = None
    classification: Annotated[str, StringConstraints(max_length=2000)] | None = None


class ContractExpectation(Body):
    """One expectation (a named rule) of a data-contract."""

    name: Name
    description: Annotated[str, StringConstraints(max_length=2000)] | None = None
    rule: Annotated[str, StringConstraints(max_length=2000)] | None = None


class ProductContract(Body):
    """The JSON data-contract of a product: columns and expectations."""

    columns: list[ContractColumn] | None = Field(None, max_length=500)
    expectations: list[ContractExpectation] | None = Field(None, max_length=100)


class ProductCreate(Body):
    """Body of ``POST /governance/products``."""

    name: Name
    description: Description | None = None
    domain_id: Ident | None = None
    status: ProductStatus = "draft"
    owners: list[OwnerSpec] | None = Field(None, max_length=50)
    member_asset_ids: list[Ident] | None = Field(None, max_length=2000)
    sla: ProductSla | None = None
    contract: ProductContract | None = None


class ProductUpdate(Body):
    """Body of ``PATCH /governance/products/{id}`` (members and owners have their own routes)."""

    name: Name | None = None
    description: Description | None = None
    domain_id: Ident | None = None
    status: ProductStatus | None = None
    sla: ProductSla | None = None
    contract: ProductContract | None = None


class ProductMembers(Body):
    """Body of ``PUT /governance/products/{id}/members``: the complete member set."""

    asset_ids: list[Ident] = Field(max_length=2000)


class ProductMember(Body):
    """Body of ``POST /governance/products/{id}/members``."""

    asset_id: Ident


# ============================================================================================================================ classifications and review
class ClassificationCreate(Body):
    """Body of ``POST /governance/classifications``."""

    name: Name
    category: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{1,31}$")] = "custom"
    level: int = Field(1, ge=0, le=5, description="0 (public) to 5 (most sensitive)")
    description: Annotated[str, StringConstraints(max_length=2000)] | None = None
    color: Color | None = None


class ClassificationUpdate(Body):
    """Body of ``PATCH /governance/classifications/{id}`` (built-in labels: description and color only)."""

    name: Name | None = None
    category: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{1,31}$")] | None = None
    level: int | None = Field(None, ge=0, le=5)
    description: Annotated[str, StringConstraints(max_length=2000)] | None = None
    color: Color | None = None


class ClassificationScan(Body):
    """Body of ``POST /governance/classifications/scan``."""

    workspace_id: Ident | None = None
    scan_id: Ident | None = None


class ClassificationAssign(Body):
    """Body of ``POST /governance/assets/{id}/classifications``."""

    classification: Ident = Field(description="a classification id or name")
    comment: Comment | None = None


class ReviewDecision(Body):
    """Optional body of a single approve / reject decision (review queue, change request, violation acknowledgement)."""

    comment: Comment | None = None


class ReviewBulk(Body):
    """Body of ``POST /governance/review-queue/bulk``."""

    action: Literal["approve", "reject"]
    ids: list[Annotated[str, StringConstraints(min_length=1, max_length=64)]] = Field(min_length=1, max_length=500)
    comment: Comment | None = None


# ============================================================================================================================ certification and changes
class CertificationPut(Body):
    """Body of ``PUT .../certification``."""

    state: CertState
    comment: Comment | None = None


class ChangeCreate(Body):
    """Body of ``POST /governance/changes``; ``payload`` depends on ``type`` (see the contract)."""

    type: ChangeType
    subject_type: ChangeSubject
    subject_id: Ident
    payload: dict[str, Any] = Field(default_factory=dict)
    reviewers: list[Ident] | None = Field(None, max_length=20)
    comment: Comment | None = None


class ChangeComment(Body):
    """Body of ``POST /governance/changes/{id}/comments``."""

    text: Annotated[str, StringConstraints(min_length=1, max_length=4000)]


class ChangeReject(Body):
    """Body of ``POST /governance/changes/{id}/reject``: the reason is required."""

    comment: Annotated[str, StringConstraints(min_length=1, max_length=2000)]


# ============================================================================================================================ policies
class PolicyCreate(Body):
    """Body of ``POST /governance/policies``. ``mode`` is accepted only to refuse ``enforce`` with a remediation (governance observes only)."""

    name: Name
    description: Annotated[str, StringConstraints(max_length=4000)] | None = None
    rule_type: RuleType
    params: dict[str, Any] = Field(default_factory=dict, description="parameters of the rule type (GET /governance/policies/rule-types)")
    severity: Severity = "medium"
    mode: Literal["observe", "enforce"] = "observe"
    enabled: bool = True


class PolicyUpdate(Body):
    """Body of ``PATCH /governance/policies/{id}`` (``rule_type`` cannot change)."""

    name: Name | None = None
    description: Annotated[str, StringConstraints(max_length=4000)] | None = None
    params: dict[str, Any] | None = None
    severity: Severity | None = None
    mode: Literal["observe", "enforce"] | None = None
    enabled: bool | None = None


class PolicyPreview(Body):
    """Body of ``POST /governance/policies/preview``."""

    rule_type: RuleType
    params: dict[str, Any] = Field(default_factory=dict)
    severity: Severity = "medium"
    limit: int = Field(100, ge=1, le=500)

