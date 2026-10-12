# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Governance tables (migration stream ``governance``).

Everything here is *about* assets, never part of the asset graph itself: an asset is referenced by its stable ``ast_`` id, and the rows follow an asset when two assets are merged
(the registry's merge hook, see :mod:`olcf.intelligence.governance.hooks`). No table has a foreign key to ``assets`` (the graph owns that table and may be rebuilt), so a missing
asset is tolerated everywhere and reads simply leave it out.

* ``gov_terms`` / ``gov_term_links`` - the business glossary and its links to assets and data products (``source`` manual | suggested, review ``status``).
* ``gov_domains`` / ``gov_asset_domains`` - the domain hierarchy and the direct domain of an asset (one per asset).
* ``gov_products`` / ``gov_product_members`` - data products with SLA, contract and member assets.
* ``gov_owners`` - owners, stewards, custodians and contacts of assets, domains, products and terms (OLCF users or external contacts).
* ``gov_classifications`` / ``gov_class_assignments`` - the classification catalog and the labels on assets (``suggested`` -> ``approved`` | ``rejected``). Inherited labels are
  **computed**, never stored.
* ``gov_certifications`` - the certification state of an asset, product or term, with the schema fingerprint it was certified against.
* ``gov_changes`` - change requests (certify, classification change, term link, ownership change, policy exception, deprecate) and their review trail.
* ``gov_policies`` / ``gov_violations`` - observe-mode policies and the violations they report.
* ``gov_signals`` - a small durable work queue (scan finished, schema changed) processed by the leader-only governance loop, so event publishers never wait for governance.

Portable between SQLite and PostgreSQL: plain columns and JSON documents, no vendor operators.
"""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, Float, Index, Integer, String, Table, Text, UniqueConstraint

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, BigPK, metadata

gov_terms = Table(
    "gov_terms", metadata,
    Column("id", ID, primary_key=True),                                   # trm_<ULID>
    Column("glossary", String(64), nullable=False, default="default"),    # glossary namespace; names are unique within it
    Column("name", String(255), nullable=False),
    Column("name_key", String(255), nullable=False),                      # lower-case name (uniqueness, search)
    Column("definition", Text, nullable=False, default=""),               # Markdown
    Column("abbreviation", String(64)),
    Column("synonyms", JSON, nullable=False, default=list),
    Column("status", String(16), nullable=False, default="draft"),        # draft | approved | deprecated
    Column("parent_id", ID),
    Column("domain_id", ID),
    Column("related", JSON, nullable=False, default=list),                # related term ids
    Column("etag", String(32), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("glossary", "name_key", name="uq_gov_terms_name"),
    Index("ix_gov_terms_parent", "parent_id"),
    Index("ix_gov_terms_domain", "domain_id"),
    Index("ix_gov_terms_status", "status"),
)

gov_term_links = Table(
    "gov_term_links", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("term_id", ID, nullable=False),
    Column("subject_type", String(12), nullable=False),                   # asset | product
    Column("subject_id", ID, nullable=False),
    Column("source", String(12), nullable=False, default="manual"),       # manual | suggested
    Column("status", String(12), nullable=False, default="approved"),     # approved | pending | rejected | unlinked
    Column("confidence", Float),
    Column("evidence", JSON, nullable=False, default=dict),
    Column("comment", Text),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("reviewed_by", ID),
    Column("reviewed_at", TS),
    Column("review_comment", Text),
    UniqueConstraint("term_id", "subject_type", "subject_id", name="uq_gov_term_links"),
    Index("ix_gov_term_links_subject", "subject_type", "subject_id"),
    Index("ix_gov_term_links_status", "status", "id"),
)

gov_domains = Table(
    "gov_domains", metadata,
    Column("id", ID, primary_key=True),                                   # dom_<ULID>
    Column("name", String(255), nullable=False),
    Column("name_key", String(255), nullable=False),
    Column("description", Text, nullable=False, default=""),
    Column("parent_id", ID),
    Column("etag", String(32), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("name_key", name="uq_gov_domains_name"),
    Index("ix_gov_domains_parent", "parent_id"),
)

gov_asset_domains = Table(
    "gov_asset_domains", metadata,
    Column("asset_id", ID, primary_key=True),
    Column("domain_id", ID, nullable=False),
    Column("assigned_by", ID),
    Column("assigned_at", TS, nullable=False),
    Index("ix_gov_asset_domains_domain", "domain_id"),
)

gov_products = Table(
    "gov_products", metadata,
    Column("id", ID, primary_key=True),                                   # dpr_<ULID>
    Column("name", String(255), nullable=False),
    Column("name_key", String(255), nullable=False),
    Column("description", Text, nullable=False, default=""),
    Column("domain_id", ID),
    Column("status", String(16), nullable=False, default="draft"),        # draft | published | deprecated
    Column("sla", JSON, nullable=False, default=dict),                    # {"freshness_hours": number, "availability": text}
    Column("contract", JSON, nullable=False, default=dict),               # {"columns": [...], "expectations": [...]}
    Column("etag", String(32), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("name_key", name="uq_gov_products_name"),
    Index("ix_gov_products_domain", "domain_id"),
    Index("ix_gov_products_status", "status"),
)

gov_product_members = Table(
    "gov_product_members", metadata,
    Column("product_id", ID, primary_key=True),
    Column("asset_id", ID, primary_key=True),
    Column("added_by", ID),
    Column("added_at", TS, nullable=False),
    Index("ix_gov_product_members_asset", "asset_id"),
)

gov_owners = Table(
    "gov_owners", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("subject_type", String(12), nullable=False),                   # asset | domain | product | term
    Column("subject_id", ID, nullable=False),
    Column("role", String(12), nullable=False),                           # owner | steward | custodian | contact
    Column("owner_key", String(320), nullable=False),                     # "u:<principal id>" | "e:<e-mail>" | "n:<name>"
    Column("kind", String(12), nullable=False),                           # user | external
    Column("user_id", ID),
    Column("name", String(255), nullable=False),
    Column("email", String(255)),
    Column("notes", Text),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    UniqueConstraint("subject_type", "subject_id", "role", "owner_key", name="uq_gov_owners"),
    Index("ix_gov_owners_subject", "subject_type", "subject_id"),
    Index("ix_gov_owners_user", "user_id"),
    Index("ix_gov_owners_email", "email"),
)

gov_classifications = Table(
    "gov_classifications", metadata,
    Column("id", ID, primary_key=True),                                   # cls_<ULID>
    Column("name", String(128), nullable=False),
    Column("name_key", String(128), nullable=False),
    Column("category", String(32), nullable=False),                       # sensitivity | pii | regulatory | financial | security | custom
    Column("level", Integer, nullable=False, default=0),                  # 0 (public) .. 5 (most sensitive)
    Column("description", Text, nullable=False, default=""),
    Column("color", String(16)),
    Column("builtin", Boolean, nullable=False, default=False),
    Column("etag", String(32), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("name_key", name="uq_gov_classifications_name"),
)

gov_class_assignments = Table(
    "gov_class_assignments", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("class_id", ID, nullable=False),
    Column("asset_id", ID, nullable=False),
    Column("source", String(12), nullable=False),                         # manual | auto  (inherited labels are computed, never stored)
    Column("status", String(12), nullable=False),                         # suggested | approved | rejected
    Column("confidence", Float),
    Column("evidence", JSON, nullable=False, default=dict),               # rule names and evidence kinds; never sample values
    Column("comment", Text),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("reviewed_by", ID),
    Column("reviewed_at", TS),
    Column("review_comment", Text),
    UniqueConstraint("asset_id", "class_id", name="uq_gov_class_assignments"),
    Index("ix_gov_class_assignments_status", "status", "id"),
    Index("ix_gov_class_assignments_class", "class_id", "status"),
)

gov_certifications = Table(
    "gov_certifications", metadata,
    Column("subject_type", String(12), primary_key=True),                 # asset | product | term
    Column("subject_id", ID, primary_key=True),
    Column("state", String(12), nullable=False),                          # none | draft | certified | deprecated
    Column("certified_by", ID),
    Column("certified_at", TS),
    Column("comment", Text),
    Column("schema_fingerprint", String(64)),                             # assets: hash of the column list the certification was given against
    Column("invalidated_at", TS),
    Column("invalidated_reason", Text),
    Column("etag", String(32), nullable=False),
    Column("updated_by", ID),
    Column("updated_at", TS, nullable=False),
    Index("ix_gov_certifications_state", "state"),
)

gov_changes = Table(
    "gov_changes", metadata,
    Column("id", ID, primary_key=True),                                   # chg_<ULID>
    Column("type", String(32), nullable=False),
    Column("subject_type", String(12), nullable=False),                   # asset | product | term | domain | violation
    Column("subject_id", ID, nullable=False),
    Column("workspace_id", ID),                                           # the workspace of the subject asset (NULL: org-level subject)
    Column("requester_id", ID, nullable=False),
    Column("requester_name", String(255), nullable=False),
    Column("payload", JSON, nullable=False, default=dict),
    Column("status", String(12), nullable=False, default="pending"),      # pending | approved | rejected | withdrawn
    Column("reviewers", JSON, nullable=False, default=list),
    Column("comments", JSON, nullable=False, default=list),               # [{"at","by","by_name","text","action"}]
    Column("decided_by", ID),
    Column("decided_name", String(255)),
    Column("decided_at", TS),
    Column("decision_comment", Text),
    Column("applied_at", TS),
    Column("apply_result", JSON, nullable=False, default=dict),
    Column("etag", String(32), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("ix_gov_changes_status", "status", "id"),
    Index("ix_gov_changes_subject", "subject_type", "subject_id"),
    Index("ix_gov_changes_requester", "requester_id"),
)

gov_policies = Table(
    "gov_policies", metadata,
    Column("id", ID, primary_key=True),                                   # plc_<ULID>
    Column("name", String(255), nullable=False),
    Column("name_key", String(255), nullable=False),
    Column("description", Text, nullable=False, default=""),
    Column("rule_type", String(32), nullable=False),
    Column("params", JSON, nullable=False, default=dict),
    Column("severity", String(12), nullable=False, default="medium"),
    Column("mode", String(12), nullable=False, default="observe"),        # observe only; "enforce" is refused
    Column("enabled", Boolean, nullable=False, default=True),
    Column("etag", String(32), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("last_evaluated_at", TS),
    Column("last_result", JSON, nullable=False, default=dict),
    UniqueConstraint("name_key", name="uq_gov_policies_name"),
)

gov_violations = Table(
    "gov_violations", metadata,
    Column("id", ID, primary_key=True),                                   # vio_<ULID>
    Column("policy_id", ID, nullable=False),
    Column("violation_key", String(64), nullable=False),                  # sha256 over (subject, discriminator): the same finding is one row
    Column("subject_type", String(12), nullable=False),                   # asset | product
    Column("subject_id", ID, nullable=False),
    Column("workspace_id", ID),
    Column("severity", String(12), nullable=False),
    Column("message", Text, nullable=False),
    Column("evidence", JSON, nullable=False, default=dict),
    Column("status", String(14), nullable=False, default="open"),         # open | acknowledged | excepted | resolved
    Column("first_seen_at", TS, nullable=False),
    Column("last_seen_at", TS, nullable=False),
    Column("resolved_at", TS),
    Column("reopen_count", Integer, nullable=False, default=0),
    Column("acknowledged_by", ID),
    Column("acknowledged_at", TS),
    Column("ack_comment", Text),
    Column("exception", JSON),                                            # {"until","change_id","reason","approved_by"}
    Column("etag", String(32), nullable=False),
    UniqueConstraint("policy_id", "violation_key", name="uq_gov_violations_key"),
    Index("ix_gov_violations_status", "status", "severity"),
    Index("ix_gov_violations_subject", "subject_type", "subject_id"),
    Index("ix_gov_violations_ws", "workspace_id"),
)

gov_signals = Table(
    "gov_signals", metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("kind", String(32), nullable=False),                           # scan_finished | schema_changed | evaluate
    Column("payload", JSON, nullable=False, default=dict),
    Column("status", String(12), nullable=False, default="pending"),      # pending | done | failed
    Column("attempts", Integer, nullable=False, default=0),
    Column("error", Text),
    Column("created_at", TS, nullable=False),
    Column("processed_at", TS),
    Index("ix_gov_signals_status", "status", "seq"),
)

GOVERNANCE_TABLES = [gov_terms, gov_term_links, gov_domains, gov_asset_domains, gov_products, gov_product_members, gov_owners, gov_classifications, gov_class_assignments,
                     gov_certifications, gov_changes, gov_policies, gov_violations, gov_signals]

register(Migration("governance", 1, "glossary, domains, data products, owners, classifications, certification, change requests, policies and violations",
                   up=lambda c: create_tables(c, GOVERNANCE_TABLES), down=lambda c: drop_tables(c, reversed(GOVERNANCE_TABLES))))
