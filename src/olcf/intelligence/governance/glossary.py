# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The business glossary: terms, their hierarchy, and their links to assets and data products.

*Terms* (``trm_``) have a name that is unique (case-insensitive) within a ``glossary`` namespace, a Markdown definition, an abbreviation, synonyms, a status
(``draft`` | ``approved`` | ``deprecated``), an optional parent term (hierarchy; cycles are refused; a parent must live in the same glossary), related terms, a domain, and owners /
stewards (see :mod:`ownership`). Only an approver (``governance.approve``) can make a term ``approved``.

*Links* connect a term to an asset (a table, a column, a measure, a report ...) or to a data product. Each link has a ``source`` (``manual`` | ``suggested``) and a review
``status`` (``approved`` | ``pending`` | ``rejected`` | ``unlinked``). A link made by a person is approved at once; a link proposed by :meth:`GlossaryManager.suggest_for_assets`
(an *exact* match of the asset name with a term name, synonym or abbreviation) waits in the review queue as ``pending`` and is never approved by the system. A rejected suggestion
is remembered and not proposed again. ``unlinked`` keeps the history of a link that was removed (re-linking reuses the row).

Deleting a term is soft by default: its status becomes ``deprecated`` and every link is unlinked; ``hard=true`` (``governance.approve``) removes the term, its links and every
reference to it (children move up one level).
"""

from __future__ import annotations

import builtins
import logging
import re
from typing import Any

from sqlalchemy import String, cast, delete, func, insert, or_, select, update

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, new_etag
from olcf.intelligence.governance.common import (
    Base,
    chunks,
    clean_name,
    clean_str_list,
    clean_text,
    compact_asset,
    like_escape,
    name_key,
)
from olcf.intelligence.governance.schema import gov_certifications, gov_owners, gov_term_links, gov_terms
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.intelligence.governance")

TERM_STATUSES = ("draft", "approved", "deprecated")
LINK_STATUSES = ("approved", "pending", "rejected", "unlinked")
LINKABLE_TYPES = ("table", "view", "model_table", "column", "measure", "report", "semantic_model", "dashboard", "visual", "file", "lakehouse")
#: Asset types the suggestion pass looks at (names of containers such as workspaces say nothing about a business concept).
SUGGEST_TYPES = ("table", "view", "model_table", "column", "measure", "report", "dashboard", "semantic_model")
MAX_SUGGESTIONS_PER_RUN = 2000
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def normalize_phrase(text: str) -> str:
    """A name reduced to its words: ``CustomerLifetimeValue``, ``customer_lifetime_value`` and ``Customer lifetime value`` all give ``customer lifetime value``."""
    spaced = _CAMEL.sub(" ", text)
    return " ".join(w for w in re.split(r"[^0-9A-Za-z]+", spaced.lower()) if w)


class GlossaryManager(Base):
    """Terms and their links (see the module docstring)."""

    # ------------------------------------------------------------------------------------------------------------------------ shaping
    def _public(self, row: dict[str, Any], owners: builtins.list[dict[str, Any]] | None, cert: str | None, link_count: int) -> dict[str, Any]:
        return {"id": row["id"], "glossary": row["glossary"], "name": row["name"], "definition": row["definition"], "abbreviation": row["abbreviation"],
                "synonyms": row["synonyms"] or [], "status": row["status"], "certification": cert or "none", "parent_id": row["parent_id"], "domain_id": row["domain_id"],
                "related": row["related"] or [], "owners": owners or [], "link_count": link_count, "etag": row["etag"], "created_by": row["created_by"],
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def _decorate_many(self, rows: builtins.list[dict[str, Any]]) -> builtins.list[dict[str, Any]]:
        ids = [r["id"] for r in rows]
        owners = {k: [self.gov.owners.public(o) for o in v] for k, v in self.gov.owners.direct("term", ids).items()}
        certs = self.gov.certification.states("term", ids)
        counts: dict[str, int] = {}
        t = gov_term_links.c
        with self.db.connect() as c:
            for part in chunks(ids):
                for tid, n in c.execute(select(t.term_id, func.count()).where(t.term_id.in_(part), t.status == "approved").group_by(t.term_id)):
                    counts[tid] = int(n)
        return [self._public(r, owners.get(r["id"]), certs.get(r["id"]), counts.get(r["id"], 0)) for r in rows]

    def row(self, term_id: str) -> dict[str, Any]:
        """The raw row of a term (``NotFound`` if absent)."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_terms).where(gov_terms.c.id == term_id))
        if r is None:
            raise NotFound(f"glossary term '{term_id}' does not exist")
        return r

    def refs(self, ids: builtins.list[str]) -> dict[str, dict[str, Any]]:
        """``{id: TermRef}`` for the existing ids."""
        out: dict[str, dict[str, Any]] = {}
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(ids))):
                for r in fetch_all(c, select(gov_terms.c.id, gov_terms.c.name, gov_terms.c.status).where(gov_terms.c.id.in_(part))):
                    out[r["id"]] = {"id": r["id"], "name": r["name"], "status": r["status"]}
        return out

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def list(self, principal: Principal, *, q: str | None = None, status: str | None = None, glossary: str | None = None, domain_id: str | None = None,
             parent_id: str | None = None, certification: str | None = None, owner: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Terms matching the filters, by id (creation order), with a keyset cursor."""
        self.org_reader(principal)
        t = gov_terms.c
        stmt = select(gov_terms)
        if status:
            bad = [s for s in status.split(",") if s not in TERM_STATUSES]
            if bad:
                raise BadRequest(f"status must be one of {list(TERM_STATUSES)}")
            stmt = stmt.where(t.status.in_(status.split(",")))
        if glossary:
            stmt = stmt.where(t.glossary == glossary)
        if domain_id:
            stmt = stmt.where(t.domain_id == domain_id)
        if parent_id:
            stmt = stmt.where(t.parent_id.is_(None) if parent_id == "root" else t.parent_id == parent_id)
        if q:
            for w in [x for x in q.lower().split() if x][:6]:
                like = like_escape(w)
                stmt = stmt.where(or_(t.name_key.like(like, escape="\\"), func.lower(t.definition).like(like, escape="\\"), func.lower(cast(t.synonyms, String)).like(like, escape="\\"),
                                      func.lower(func.coalesce(t.abbreviation, "")).like(like, escape="\\")))
        if certification:
            if certification not in ("none", "draft", "certified", "deprecated"):
                raise BadRequest("certification must be one of none, draft, certified, deprecated")
            cert_ids = select(gov_certifications.c.subject_id).where(gov_certifications.c.subject_type == "term", gov_certifications.c.state == certification)
            stmt = stmt.where(t.id.not_in(select(gov_certifications.c.subject_id).where(gov_certifications.c.subject_type == "term", gov_certifications.c.state != "none"))
                              if certification == "none" else t.id.in_(cert_ids))
        if owner:
            ids = self.gov.owners.subjects_for("term", owner)
            stmt = stmt.where(t.id.in_(sorted(ids)) if ids else t.id == "")
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id > cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return self._decorate_many(rows[:limit]), nxt

    def get(self, principal: Principal, term_id: str) -> dict[str, Any]:
        """One term with its ancestors, children and related terms."""
        self.org_reader(principal)
        r = self.row(term_id)
        out = self._decorate_many([r])[0]
        ancestors: builtins.list[dict[str, Any]] = []
        seen = {r["id"]}
        cur = r["parent_id"]
        while cur and cur not in seen and len(ancestors) < 20:
            seen.add(cur)
            pr = fetch_one_safe(self.db, gov_terms, cur)
            if pr is None:
                break
            ancestors.append({"id": pr["id"], "name": pr["name"], "status": pr["status"]})
            cur = pr["parent_id"]
        with self.db.connect() as c:
            kids = fetch_all(c, select(gov_terms.c.id, gov_terms.c.name, gov_terms.c.status).where(gov_terms.c.parent_id == r["id"]).order_by(gov_terms.c.name_key))
        ancestors.reverse()                # root first, like a breadcrumb
        refs = self.refs(r["related"] or [])
        out.update(ancestors=ancestors, children=[{"id": k["id"], "name": k["name"], "status": k["status"]} for k in kids], related_terms=[refs[i] for i in (r["related"] or []) if i in refs])
        return out

    def search(self, principal: Principal, q: str, *, limit: int = 25, include_deprecated: bool = False) -> builtins.list[dict[str, Any]]:
        """Ranked search over names, synonyms, abbreviations and definitions (exact name 100, exact synonym 90, abbreviation 80, name prefix 60, name contains 40, synonym contains 30, definition 10)."""
        self.org_reader(principal)
        needle = " ".join(q.lower().split())
        if not needle:
            raise BadRequest("q must contain at least one search term")
        t = gov_terms.c
        like = like_escape(needle)
        stmt = select(gov_terms).where(or_(t.name_key.like(like, escape="\\"), func.lower(t.definition).like(like, escape="\\"), func.lower(cast(t.synonyms, String)).like(like, escape="\\"),
                                           func.lower(func.coalesce(t.abbreviation, "")).like(like, escape="\\")))
        if not include_deprecated:
            stmt = stmt.where(t.status != "deprecated")
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.limit(500))
        scored: builtins.list[tuple[int, dict[str, Any], str]] = []
        for r in rows:
            name = r["name_key"]
            syn = [s.lower() for s in (r["synonyms"] or [])]
            abbr = (r["abbreviation"] or "").lower()
            best, how = 0, ""
            for score, why, hit in ((100, "name", name == needle), (90, "synonym", needle in syn), (80, "abbreviation", bool(abbr) and abbr == needle), (60, "name", name.startswith(needle)),
                                    (40, "name", needle in name), (30, "synonym", any(needle in s for s in syn)), (10, "definition", needle in (r["definition"] or "").lower())):
                if hit and score > best:
                    best, how = score, why
            if best:
                scored.append((best, r, how))
        scored.sort(key=lambda x: (-x[0], x[1]["name_key"]))
        top = scored[: max(1, min(int(limit), 100))]
        decorated = self._decorate_many([r for _, r, _ in top])
        return [{"term": d, "score": s, "matched_on": how} for (s, _, how), d in zip(top, decorated, strict=True)]

    # ------------------------------------------------------------------------------------------------------------------------ validation
    def _validate_parent(self, parent_id: str | None, glossary: str, self_id: str | None) -> None:
        if parent_id is None:
            return
        parent = fetch_one_safe(self.db, gov_terms, parent_id)
        if parent is None:
            raise BadRequest(f"parent term '{parent_id}' does not exist")
        if parent["glossary"] != glossary:
            raise BadRequest("a parent term must be in the same glossary", details={"parent_glossary": parent["glossary"], "glossary": glossary})
        seen = set()
        cur: str | None = parent_id
        while cur and cur not in seen:
            if self_id and cur == self_id:
                raise BadRequest("the parent chain would contain the term itself (cycle)")
            seen.add(cur)
            pr = fetch_one_safe(self.db, gov_terms, cur)
            cur = pr["parent_id"] if pr else None

    def _validate_related(self, related: Any, self_id: str | None) -> builtins.list[str]:
        ids = clean_str_list(related, "related", max_items=50, max_len=64)
        if self_id and self_id in ids:
            raise BadRequest("a term cannot be related to itself")
        if ids:
            missing = [i for i in ids if i not in self.refs(ids)]
            if missing:
                raise BadRequest(f"related term(s) do not exist: {missing}")
        return ids

    def _validate_domain(self, domain_id: str | None) -> None:
        if domain_id is not None and self.gov.domains.row_or_none(domain_id) is None:
            raise BadRequest(f"domain '{domain_id}' does not exist")

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Create a term (``governance.curate`` org-wide; ``status: approved`` needs ``governance.approve``)."""
        self.org_curator(principal)
        allowed = {"name", "definition", "abbreviation", "synonyms", "glossary", "status", "parent_id", "domain_id", "related", "owners"}
        extra = set(body) - allowed
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}", details={"allowed": sorted(allowed)})
        name = clean_name(body.get("name"), "name")
        glossary = clean_name(body.get("glossary") or "default", "glossary", max_len=64)
        status = body.get("status", "draft")
        if status not in TERM_STATUSES:
            raise BadRequest(f"status must be one of {list(TERM_STATUSES)}")
        if status == "approved":
            principal.require("governance.approve")
        self._validate_parent(body.get("parent_id"), glossary, None)
        self._validate_domain(body.get("domain_id"))
        owners = [self.gov.owners.parse(o) for o in (body.get("owners") or [])]
        now = now_iso()
        tid = new_id("trm")
        values = {"id": tid, "glossary": glossary, "name": name, "name_key": name_key(name), "definition": clean_text(body.get("definition"), "definition", max_len=20000) or "",
                  "abbreviation": (clean_name(body["abbreviation"], "abbreviation", max_len=64) if body.get("abbreviation") else None),
                  "synonyms": clean_str_list(body.get("synonyms"), "synonyms", max_items=50, max_len=255), "status": status, "parent_id": body.get("parent_id"),
                  "domain_id": body.get("domain_id"), "related": self._validate_related(body.get("related"), tid), "etag": new_etag(), "created_by": principal.id,
                  "created_at": now, "updated_at": now}
        try:
            with self.db.begin() as c:
                if fetch_one(c, select(gov_terms.c.id).where(gov_terms.c.glossary == glossary, gov_terms.c.name_key == values["name_key"])):
                    raise Conflict(f"a term named '{name}' already exists in glossary '{glossary}'")
                c.execute(insert(gov_terms).values(**values))
                for o in owners:
                    self.gov.owners._insert(c, "term", tid, o, principal.id)  # noqa: SLF001
        except Conflict:
            raise
        self.audit(principal, "governance.term_created", resource_type="term", resource_id=tid, details={"name": name, "glossary": glossary, "status": status})
        return self.get(principal, tid)

    def update(self, principal: Principal, term_id: str, body: dict[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a term; only the fields present change."""
        self.org_curator(principal)
        row = self.row(term_id)
        self.check_etag(if_match, row["etag"])
        allowed = {"name", "definition", "abbreviation", "synonyms", "status", "parent_id", "domain_id", "related"}
        extra = set(body) - allowed
        if extra:
            raise BadRequest(f"unknown or immutable field(s) {sorted(extra)}", details={"allowed": sorted(allowed)})
        vals: dict[str, Any] = {}
        if "name" in body:
            vals["name"] = clean_name(body["name"], "name")
            vals["name_key"] = name_key(vals["name"])
        if "definition" in body:
            vals["definition"] = clean_text(body["definition"], "definition", max_len=20000, allow_none=False)
        if "abbreviation" in body:
            vals["abbreviation"] = clean_name(body["abbreviation"], "abbreviation", max_len=64) if body["abbreviation"] else None
        if "synonyms" in body:
            vals["synonyms"] = clean_str_list(body["synonyms"], "synonyms", max_items=50, max_len=255)
        if "status" in body:
            if body["status"] not in TERM_STATUSES:
                raise BadRequest(f"status must be one of {list(TERM_STATUSES)}")
            if body["status"] == "approved" and row["status"] != "approved":
                principal.require("governance.approve")
            vals["status"] = body["status"]
        if "parent_id" in body:
            self._validate_parent(body["parent_id"], row["glossary"], row["id"])
            vals["parent_id"] = body["parent_id"]
        if "domain_id" in body:
            self._validate_domain(body["domain_id"])
            vals["domain_id"] = body["domain_id"]
        if "related" in body:
            vals["related"] = self._validate_related(body["related"], row["id"])
        if not vals:
            raise BadRequest("nothing to update", remediation="send at least one of " + ", ".join(sorted(allowed)))
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.db.begin() as c:
            if "name_key" in vals and fetch_one(c, select(gov_terms.c.id).where(gov_terms.c.glossary == row["glossary"], gov_terms.c.name_key == vals["name_key"], gov_terms.c.id != row["id"])):
                raise Conflict(f"a term named '{vals['name']}' already exists in glossary '{row['glossary']}'")
            n = c.execute(update(gov_terms).where(gov_terms.c.id == row["id"], gov_terms.c.etag == row["etag"]).values(**vals)).rowcount
        if n == 0:
            cur = self.row(term_id)
            raise_precondition(cur["etag"])
        self.audit(principal, "governance.term_updated", resource_type="term", resource_id=row["id"], details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "name_key"))})
        return self.get(principal, term_id)

    def delete(self, principal: Principal, term_id: str, *, hard: bool = False, if_match: str | None = None) -> dict[str, Any]:
        """Soft delete (deprecate and unlink) or, with ``hard``, remove the term for good (needs ``governance.approve``)."""
        self.org_curator(principal)
        if hard:
            principal.require("governance.approve")
        row = self.row(term_id)
        self.check_etag(if_match, row["etag"])
        now = now_iso()
        with self.db.begin() as c:
            if hard:
                n = c.execute(delete(gov_term_links).where(gov_term_links.c.term_id == row["id"])).rowcount
                c.execute(update(gov_terms).where(gov_terms.c.parent_id == row["id"]).values(parent_id=row["parent_id"], updated_at=now, etag=new_etag()))
                for other in fetch_all(c, select(gov_terms.c.id, gov_terms.c.related).where(gov_terms.c.related.is_not(None))):
                    if row["id"] in (other["related"] or []):
                        c.execute(update(gov_terms).where(gov_terms.c.id == other["id"]).values(related=[x for x in other["related"] if x != row["id"]], updated_at=now, etag=new_etag()))
                c.execute(delete(gov_owners).where(gov_owners.c.subject_type == "term", gov_owners.c.subject_id == row["id"]))
                c.execute(delete(gov_certifications).where(gov_certifications.c.subject_type == "term", gov_certifications.c.subject_id == row["id"]))
                c.execute(delete(gov_terms).where(gov_terms.c.id == row["id"]))
            else:
                n = c.execute(update(gov_term_links).where(gov_term_links.c.term_id == row["id"], gov_term_links.c.status.in_(("approved", "pending"))).values(
                    status="unlinked", reviewed_by=principal.id, reviewed_at=now, review_comment="term deleted")).rowcount
                c.execute(update(gov_terms).where(gov_terms.c.id == row["id"]).values(status="deprecated", updated_at=now, etag=new_etag()))
        self.audit(principal, "governance.term_deleted", resource_type="term", resource_id=row["id"], details={"name": row["name"], "hard": hard, "links_removed": int(n)})
        return {"id": row["id"], "status": "deleted" if hard else "deprecated", "deleted": hard, "deprecated": not hard, "links_unlinked": int(n)}

    # ------------------------------------------------------------------------------------------------------------------------ links
    def _link_public(self, principal: Principal, r: dict[str, Any], terms: dict[str, dict[str, Any]], assets_: dict[str, dict[str, Any]], products: dict[str, dict[str, Any]]) -> dict[str, Any]:
        return {"id": r["id"], "term": terms.get(r["term_id"]), "subject_type": r["subject_type"],
                "asset": compact_asset(assets_[r["subject_id"]]) if r["subject_type"] == "asset" and r["subject_id"] in assets_ else None,
                "product": products.get(r["subject_id"]) if r["subject_type"] == "product" else None, "source": r["source"], "status": r["status"], "confidence": r["confidence"],
                "evidence": r["evidence"] or {}, "comment": r["comment"], "created_by": r["created_by"], "created_at": r["created_at"], "reviewed_by": r["reviewed_by"],
                "reviewed_at": r["reviewed_at"]}

    def shape_links(self, principal: Principal, rows: builtins.list[dict[str, Any]]) -> builtins.list[dict[str, Any]]:
        """Link rows in API form; links to assets the caller cannot see are dropped."""
        terms = self.refs([r["term_id"] for r in rows])
        vis = self.visible_rows(principal, [r["subject_id"] for r in rows if r["subject_type"] == "asset"])
        products = self.gov.products.refs([r["subject_id"] for r in rows if r["subject_type"] == "product"])
        out = []
        for r in rows:
            if r["subject_type"] == "asset" and r["subject_id"] not in vis:
                continue
            out.append(self._link_public(principal, r, terms, vis, products))
        return out

    def links_of_term(self, principal: Principal, term_id: str, *, status: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Links of a term (default: approved and pending)."""
        self.org_reader(principal)
        self.row(term_id)
        wanted = (status or "approved,pending").split(",")
        if any(s not in LINK_STATUSES for s in wanted):
            raise BadRequest(f"status must be a comma list of {list(LINK_STATUSES)}")
        t = gov_term_links.c
        stmt = select(gov_term_links).where(t.term_id == term_id, t.status.in_(wanted))
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id > int(cur[0]))
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return self.shape_links(principal, rows[:limit]), nxt

    def link(self, principal: Principal, term_id: str, *, asset_id: str | None = None, product_id: str | None = None, comment: str | None = None) -> dict[str, Any]:
        """Link a term to an asset or product (manual link, approved at once). ``governance.curate`` in the asset's workspace (product: org-wide)."""
        if bool(asset_id) == bool(product_id):
            raise BadRequest("send exactly one of asset_id and product_id")
        term = self.row(term_id)
        if term["status"] == "deprecated":
            raise BadRequest(f"term '{term['name']}' is deprecated and cannot be linked", remediation="approve or re-draft the term first")
        if asset_id:
            arow = self.curate_asset(principal, asset_id)
            stype, sid, ws = "asset", arow["id"], arow["workspace_id"]
            if arow["type"] not in LINKABLE_TYPES:
                raise BadRequest(f"a {arow['type']} cannot be linked to a glossary term", details={"supported": list(LINKABLE_TYPES)})
        else:
            self.org_curator(principal)
            assert product_id
            if self.gov.products.row_or_none(product_id) is None:
                raise NotFound(f"data product '{product_id}' does not exist")
            stype, sid, ws = "product", product_id, None
        now = now_iso()
        comment = clean_text(comment, "comment", max_len=2000)
        t = gov_term_links.c
        with self.db.begin() as c:
            ex = fetch_one(c, select(gov_term_links).where(t.term_id == term["id"], t.subject_type == stype, t.subject_id == sid))
            if ex is not None and ex["status"] == "approved":
                raise Conflict(f"'{term['name']}' is already linked to this {stype}", details={"link_id": ex["id"]})
            vals = {"source": "manual", "status": "approved", "confidence": None, "comment": comment, "reviewed_by": principal.id, "reviewed_at": now, "review_comment": None}
            if ex is None:
                res = c.execute(insert(gov_term_links).values(term_id=term["id"], subject_type=stype, subject_id=sid, evidence={}, created_by=principal.id, created_at=now, **vals))
                lid = int(res.inserted_primary_key[0])
            else:
                lid = ex["id"]
                c.execute(update(gov_term_links).where(t.id == lid).values(**vals))
            row = fetch_one(c, select(gov_term_links).where(t.id == lid))
        assert row is not None
        self.audit(principal, "governance.term_linked", resource_type=stype, resource_id=sid, workspace_id=ws, details={"term_id": term["id"], "term": term["name"], "link_id": lid})
        return self.shape_links(principal, [row])[0]

    def unlink(self, principal: Principal, term_id: str, link_id: int) -> dict[str, Any]:
        """Remove a link (kept as history with status ``unlinked``)."""
        t = gov_term_links.c
        with self.db.connect() as c:
            row = fetch_one(c, select(gov_term_links).where(t.id == link_id, t.term_id == term_id))
        if row is None:
            raise NotFound(f"link {link_id} of term '{term_id}' does not exist")
        ws = None
        if row["subject_type"] == "asset":
            arow = self.curate_asset(principal, row["subject_id"])
            ws = arow["workspace_id"]
        else:
            self.org_curator(principal)
        self.unlink_row(row, principal.id)
        self.audit(principal, "governance.term_unlinked", resource_type=row["subject_type"], resource_id=row["subject_id"], workspace_id=ws, details={"term_id": term_id, "link_id": link_id})
        return {"id": link_id, "status": "unlinked"}

    def unlink_from_subject(self, principal: Principal, term_id: str, stype: str, sid: str) -> dict[str, Any]:
        """``DELETE /assets/{id}/terms/{term_id}``: unlink by (term, subject) with the caller's rights checked (``NotFound`` if no live link exists)."""
        t = gov_term_links.c
        with self.db.connect() as c:
            row = fetch_one(c, select(gov_term_links).where(t.term_id == term_id, t.subject_type == stype, t.subject_id == sid, t.status.in_(("approved", "pending"))))
        if row is None:
            raise NotFound(f"term '{term_id}' is not linked to this {stype}")
        return self.unlink(principal, term_id, row["id"])

    def unlink_row(self, row: dict[str, Any], by: str | None) -> None:
        """Mark one link row ``unlinked`` (no authorisation: callers have checked)."""
        with self.db.begin() as c:
            c.execute(update(gov_term_links).where(gov_term_links.c.id == row["id"]).values(status="unlinked", reviewed_by=by, reviewed_at=now_iso()))

    def unlink_subject(self, principal: Principal | None, term_id: str, stype: str, sid: str) -> bool:
        """Unlink by (term, subject) for approved change requests; ``True`` if a link was removed."""
        t = gov_term_links.c
        with self.db.begin() as c:
            n = c.execute(update(gov_term_links).where(t.term_id == term_id, t.subject_type == stype, t.subject_id == sid, t.status.in_(("approved", "pending"))).values(
                status="unlinked", reviewed_by=principal.id if principal else None, reviewed_at=now_iso())).rowcount
        return n > 0

    def link_subject(self, principal: Principal | None, term_id: str, stype: str, sid: str, comment: str | None = None) -> bool:
        """Create / approve a manual link by (term, subject) for approved change requests; ``True`` if anything changed."""
        term = self.row(term_id)
        if term["status"] == "deprecated":
            raise BadRequest(f"term '{term['name']}' is deprecated and cannot be linked")
        t = gov_term_links.c
        now = now_iso()
        with self.db.begin() as c:
            ex = fetch_one(c, select(gov_term_links).where(t.term_id == term_id, t.subject_type == stype, t.subject_id == sid))
            if ex is not None and ex["status"] == "approved":
                return False
            vals = {"source": "manual", "status": "approved", "comment": comment, "reviewed_by": principal.id if principal else None, "reviewed_at": now}
            if ex is None:
                c.execute(insert(gov_term_links).values(term_id=term_id, subject_type=stype, subject_id=sid, evidence={}, created_by=principal.id if principal else None, created_at=now, **vals))
            else:
                c.execute(update(gov_term_links).where(t.id == ex["id"]).values(**vals))
        return True

    # ------------------------------------------------------------------------------------------------------------------------ links seen from an asset
    def terms_of_assets(self, asset_ids: builtins.list[str], *, include_pending: bool = False) -> dict[str, builtins.list[dict[str, Any]]]:
        """Approved (optionally also pending) term links per asset, without deprecated terms: ``{asset_id: [{id, name, status, link_id, link_status, source}]}``."""
        out: dict[str, builtins.list[dict[str, Any]]] = {i: [] for i in asset_ids}
        statuses = ("approved", "pending") if include_pending else ("approved",)
        t, g = gov_term_links.c, gov_terms.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(asset_ids))):
                stmt = (select(t.id.label("link_id"), t.subject_id, t.status.label("link_status"), t.source, t.confidence, g.id, g.name, g.status)
                        .join(gov_terms, gov_terms.c.id == t.term_id).where(t.subject_type == "asset", t.subject_id.in_(part), t.status.in_(statuses), g.status != "deprecated")
                        .order_by(g.name_key))
                for r in c.execute(stmt):
                    m = r._mapping
                    out[m["subject_id"]].append({"id": m["id"], "name": m["name"], "status": m["status"], "link_id": m["link_id"], "link_status": m["link_status"], "source": m["source"],
                                                 "confidence": m["confidence"]})
        return out

    def terms_of_products(self, product_ids: builtins.list[str]) -> dict[str, builtins.list[dict[str, Any]]]:
        """Approved term links per data product."""
        out: dict[str, builtins.list[dict[str, Any]]] = {i: [] for i in product_ids}
        t, g = gov_term_links.c, gov_terms.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(product_ids))):
                stmt = (select(t.id.label("link_id"), t.subject_id, g.id, g.name, g.status).join(gov_terms, gov_terms.c.id == t.term_id)
                        .where(t.subject_type == "product", t.subject_id.in_(part), t.status == "approved", g.status != "deprecated").order_by(g.name_key))
                for r in c.execute(stmt):
                    m = r._mapping
                    out[m["subject_id"]].append({"id": m["id"], "name": m["name"], "status": m["status"], "link_id": m["link_id"]})
        return out

    def asset_ids_for_term(self, term: str) -> set[str]:
        """Assets with an approved link to the term (id or exact name)."""
        t = gov_term_links.c
        with self.db.connect() as c:
            tid = term if term.startswith("trm_") else None
            if tid is None:
                r = fetch_one(c, select(gov_terms.c.id).where(gov_terms.c.name_key == name_key(term)))
                tid = r["id"] if r else None
            if tid is None:
                return set()
            return {r[0] for r in c.execute(select(t.subject_id).where(t.term_id == tid, t.subject_type == "asset", t.status == "approved"))}

    # ------------------------------------------------------------------------------------------------------------------------ suggestions
    def suggest_for_assets(self, rows: builtins.list[dict[str, Any]]) -> dict[str, int]:
        """Propose links for assets whose name equals (as words) the name, a synonym or the abbreviation of a non-deprecated term. Existing links in any state suppress a proposal.
        Returns ``{"examined", "suggested"}``. Proposals are ``pending`` and wait in the review queue."""
        with self.db.connect() as c:
            terms = fetch_all(c, select(gov_terms).where(gov_terms.c.status != "deprecated"))
        if not terms:
            return {"examined": 0, "suggested": 0}
        index: dict[str, builtins.list[tuple[dict[str, Any], str, float]]] = {}
        for tr in terms:
            keys = [(normalize_phrase(tr["name"]), "name", 0.65)]
            keys += [(normalize_phrase(s), "synonym", 0.55) for s in tr["synonyms"] or []]
            if tr["abbreviation"]:
                keys.append((normalize_phrase(tr["abbreviation"]), "abbreviation", 0.45))
            for k, how, conf in keys:
                if len(k) >= 3:
                    index.setdefault(k, []).append((tr, how, conf))
        t = gov_term_links.c
        examined = suggested = 0
        now = now_iso()
        candidates = [r for r in rows if r["type"] in SUGGEST_TYPES and r["status"] != "removed"]
        existing: set[tuple[str, str]] = set()
        with self.db.connect() as c:
            for part in chunks([r["id"] for r in candidates]):
                for r in c.execute(select(t.term_id, t.subject_id).where(t.subject_type == "asset", t.subject_id.in_(part))):
                    existing.add((r[0], r[1]))
        for r in candidates:
            examined += 1
            hits = index.get(normalize_phrase(r["name"]))
            if not hits:
                continue
            for tr, how, conf in hits:
                if (tr["id"], r["id"]) in existing or suggested >= MAX_SUGGESTIONS_PER_RUN:
                    continue
                try:
                    with self.db.begin() as c:
                        c.execute(insert(gov_term_links).values(term_id=tr["id"], subject_type="asset", subject_id=r["id"], source="suggested", status="pending", confidence=conf,
                                                                evidence={"rule": f"name-matches-{how}", "matched": how}, created_by=None, created_at=now))
                    existing.add((tr["id"], r["id"]))
                    suggested += 1
                except Exception as exc:  # noqa: BLE001 - a concurrent run inserted the same suggestion (unique key): fine
                    log.debug("term suggestion skipped: %s", exc)
        return {"examined": examined, "suggested": suggested}

    def suggest(self, principal: Principal, workspace_id: str | None = None) -> dict[str, Any]:
        """``POST /governance/glossary/suggest-links``: run the suggestion pass over the visible assets (of one workspace if given)."""
        if workspace_id:
            principal.require("governance.curate", workspace_id=workspace_id)
        else:
            principal.require("governance.curate")
        from olcf.intelligence.schema import assets

        stmt = self.intel.scope_filter(select(assets).where(assets.c.status != "removed", assets.c.type.in_(SUGGEST_TYPES)), principal, workspace_id).limit(20000)
        with self.db.connect() as c:
            rows = fetch_all(c, stmt)
        out = self.suggest_for_assets(rows)
        if out["suggested"]:
            self.emit("governance.review.requested", {"kind": "term_link", "count": out["suggested"], "source": "glossary-suggestions"}, workspace_id=workspace_id)
        return out


def fetch_one_safe(db: Any, table: Any, ident: str) -> dict[str, Any] | None:
    """Row of ``table`` by primary key ``id``."""
    with db.connect() as c:
        return fetch_one(c, select(table).where(table.c.id == ident))


def raise_precondition(current_etag: str) -> None:
    """Raise the 412 used when a conditional update lost a race."""
    from olcf.core.errors import PreconditionFailed

    raise PreconditionFailed("the resource was modified concurrently", details={"current_etag": current_etag})


__all__ = ["GlossaryManager", "normalize_phrase", "raise_precondition"]
