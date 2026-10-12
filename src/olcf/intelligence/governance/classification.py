# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Classifications: the label catalog, assignments to assets, the review queue and the exposure query.

**Catalog.** A label (``cls_``) has a name (unique, case-insensitive), a category (``sensitivity``, ``pii``, ``regulatory``, ``financial``, ``security``, ``custom`` ...), a sensitivity
``level`` 0..5, a description and a colour. The built-in set (Public, Internal, Confidential, Restricted, PII, PHI, PCI, Financial, Credentials) is seeded idempotently at start-up;
built-in labels cannot be renamed, re-levelled or deleted.

**Assignments.** One row per (asset, label): ``source`` ``manual`` (a person) or ``auto`` (suggested from discovery), ``status`` ``suggested`` | ``approved`` | ``rejected``, a confidence
and *evidence* (rule names and evidence kinds - **never sample values**). Only ``approved`` rows count anywhere (filters, policies, quality, propagation). **Nothing is approved
automatically**: discovery's ``attrs.suggested_classifications`` are turned into ``suggested`` rows after scans and on demand, and wait in the review queue; a rejected suggestion
is remembered and not proposed again. With the setting ``governance.classification_four_eyes`` an approval needs ``governance.approve`` (not just ``governance.curate``) and cannot be
given by the person who made the suggestion, unless ``governance.allow_self_approval`` is on.

**Inherited labels** are not stored: see :mod:`propagation`. The exposure query answers "where does data labelled X end up?" from the same computation.
"""

from __future__ import annotations

import builtins
import re
from collections.abc import Iterable
from typing import Any

from sqlalchemy import and_, delete, false, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, OlcfError
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, new_etag
from olcf.intelligence.governance import flow
from olcf.intelligence.governance.common import (
    HIDDEN,
    Base,
    chunks,
    clean_color,
    clean_name,
    clean_text,
    compact_asset,
    name_key,
)
from olcf.intelligence.governance.glossary import raise_precondition
from olcf.intelligence.governance.schema import (
    gov_class_assignments,
    gov_classifications,
    gov_product_members,
    gov_products,
    gov_term_links,
)
from olcf.intelligence.schema import assets
from olcf.security.rbac import Principal

BUILTINS: tuple[tuple[str, str, int, str, str], ...] = (
    ("Public", "sensitivity", 0, "Information approved for public release.", "#2e7d32"),
    ("Internal", "sensitivity", 1, "For use inside the organisation; not for publication.", "#1565c0"),
    ("Confidential", "sensitivity", 3, "Business-sensitive information shared on a need-to-know basis.", "#ef6c00"),
    ("Restricted", "sensitivity", 4, "Highly sensitive; disclosure would cause serious harm.", "#c62828"),
    ("PII", "pii", 3, "Personally identifiable information.", "#6a1b9a"),
    ("PHI", "regulatory", 4, "Protected health information: health data about an identifiable person.", "#ad1457"),
    ("PCI", "regulatory", 4, "Payment card data in scope of PCI DSS.", "#b71c1c"),
    ("Financial", "financial", 3, "Financial records, bank accounts and account numbers.", "#00695c"),
    ("Credentials", "security", 5, "Passwords, keys, tokens and other secrets.", "#212121"),
)
#: Discovery's detector labels (``olcf.intelligence.discovery.detect``) -> catalog labels.
DETECTOR_MAP: dict[str, str] = {
    "email": "PII", "phone": "PII", "national_id": "PII", "person_name": "PII", "address": "PII", "date_of_birth": "PII", "ip_address": "PII", "geolocation": "PII",
    "credit_card": "PCI", "iban": "Financial", "bank_account": "Financial", "financial": "Financial", "credential": "Credentials", "special_category": "Restricted",
}
_CATEGORY = re.compile(r"^[a-z][a-z0-9_-]{1,31}$")
REVIEW_KINDS = ("classification", "term_link")
MAX_BULK = 500


def _item_id(kind: str, n: int) -> str:
    return f"{kind}:{n}"


def parse_item_id(item_id: str) -> tuple[str, int]:
    """Split a review-queue item id (``classification:12``) into kind and row id."""
    kind, _, num = item_id.partition(":")
    if kind not in REVIEW_KINDS or not num.isdigit():
        raise BadRequest(f"'{item_id}' is not a review-queue item id", remediation="ids look like 'classification:12' or 'term_link:5'")
    return kind, int(num)


class ClassificationManager(Base):
    """The classification catalog, assignments, ingestion of suggestions, review and exposure."""

    # ------------------------------------------------------------------------------------------------------------------------ catalog
    def ensure_builtins(self) -> int:
        """Seed the built-in labels that are missing (idempotent, safe on every replica). Returns how many were created."""
        made = 0
        now = now_iso()
        for name, category, level, desc, color in BUILTINS:
            with self.db.connect() as c:
                if fetch_one(c, select(gov_classifications.c.id).where(gov_classifications.c.name_key == name_key(name))):
                    continue
            try:
                with self.db.begin() as c:
                    c.execute(insert(gov_classifications).values(id=new_id("cls"), name=name, name_key=name_key(name), category=category, level=level, description=desc, color=color,
                                                                 builtin=True, etag=new_etag(), created_at=now, updated_at=now))
                made += 1
            except IntegrityError:  # another replica seeded it first
                continue
        return made

    def row_or_none(self, ref: str) -> dict[str, Any] | None:
        """A label by id or name."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_classifications).where(gov_classifications.c.id == ref))
            return r or fetch_one(c, select(gov_classifications).where(gov_classifications.c.name_key == name_key(ref)))

    def resolve(self, ref: Any) -> dict[str, Any]:
        """A label by id or name (``NotFound`` otherwise)."""
        if not isinstance(ref, str) or not ref.strip():
            raise BadRequest("classification must be a label id or name")
        r = self.row_or_none(ref.strip())
        if r is None:
            raise NotFound(f"classification '{ref}' does not exist", remediation="list the catalog with GET /v1/governance/classifications")
        return r

    @staticmethod
    def ref_of(row: dict[str, Any]) -> dict[str, Any]:
        """``ClassRef``."""
        return {"id": row["id"], "name": row["name"], "category": row["category"], "level": row["level"], "color": row["color"]}

    def refs(self, ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """``{id: ClassRef}``."""
        out: dict[str, dict[str, Any]] = {}
        want = list(dict.fromkeys(ids))
        with self.db.connect() as c:
            for part in chunks(want):
                for r in fetch_all(c, select(gov_classifications).where(gov_classifications.c.id.in_(part))):
                    out[r["id"]] = self.ref_of(r)
        return out

    def _public(self, r: dict[str, Any], count: int) -> dict[str, Any]:
        return {**self.ref_of(r), "description": r["description"], "builtin": bool(r["builtin"]), "assignment_count": count, "etag": r["etag"], "created_at": r["created_at"],
                "updated_at": r["updated_at"]}

    def _counts(self, ids: builtins.list[str]) -> dict[str, int]:
        g = gov_class_assignments.c
        out: dict[str, int] = {}
        with self.db.connect() as c:
            for part in chunks(ids):
                for cid, n in c.execute(select(g.class_id, func.count()).where(g.class_id.in_(part), g.status != "rejected").group_by(g.class_id)):
                    out[cid] = int(n)
        return out

    def list(self, principal: Principal) -> builtins.list[dict[str, Any]]:
        """The whole catalog (it is small), by level then name."""
        self.org_reader(principal)
        with self.db.connect() as c:
            rows = fetch_all(c, select(gov_classifications).order_by(gov_classifications.c.level, gov_classifications.c.name_key))
        counts = self._counts([r["id"] for r in rows])
        return [self._public(r, counts.get(r["id"], 0)) for r in rows]

    def get(self, principal: Principal, ref: str) -> dict[str, Any]:
        """One label."""
        self.org_reader(principal)
        r = self.resolve(ref)
        return self._public(r, self._counts([r["id"]]).get(r["id"], 0))

    def _clean_category(self, value: Any) -> str:
        if not isinstance(value, str) or not _CATEGORY.match(value):
            raise BadRequest("category must be a short lower-case word such as sensitivity, pii, regulatory, financial, security or custom")
        return value

    @staticmethod
    def _clean_level(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 5:
            raise BadRequest("level must be an integer from 0 (public) to 5 (most sensitive)")
        return value

    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Create a custom label (org-wide ``governance.curate``)."""
        self.org_curator(principal)
        extra = set(body) - {"name", "category", "level", "description", "color"}
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}", details={"allowed": ["name", "category", "level", "description", "color"]})
        name = clean_name(body.get("name"), "name", max_len=128)
        now = now_iso()
        cid = new_id("cls")
        with self.db.begin() as c:
            if fetch_one(c, select(gov_classifications.c.id).where(gov_classifications.c.name_key == name_key(name))):
                raise Conflict(f"a classification named '{name}' already exists")
            c.execute(insert(gov_classifications).values(id=cid, name=name, name_key=name_key(name), category=self._clean_category(body.get("category", "custom")),
                                                         level=self._clean_level(body.get("level", 1)), description=clean_text(body.get("description"), "description", max_len=2000) or "",
                                                         color=clean_color(body.get("color")), builtin=False, etag=new_etag(), created_at=now, updated_at=now))
        self.audit(principal, "governance.classification_created", resource_type="classification", resource_id=cid, details={"name": name})
        return self.get(principal, cid)

    def update(self, principal: Principal, ref: str, body: dict[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a label (built-in labels: description and colour only)."""
        self.org_curator(principal)
        row = self.resolve(ref)
        self.check_etag(if_match, row["etag"])
        allowed = {"description", "color"} if row["builtin"] else {"name", "category", "level", "description", "color"}
        extra = set(body) - allowed
        if extra:
            raise BadRequest(f"cannot change {sorted(extra)}" + (" of a built-in classification" if row["builtin"] else ""), details={"allowed": sorted(allowed)})
        vals: dict[str, Any] = {}
        if "name" in body:
            vals["name"] = clean_name(body["name"], "name", max_len=128)
            vals["name_key"] = name_key(vals["name"])
        if "category" in body:
            vals["category"] = self._clean_category(body["category"])
        if "level" in body:
            vals["level"] = self._clean_level(body["level"])
        if "description" in body:
            vals["description"] = clean_text(body["description"], "description", max_len=2000, allow_none=False)
        if "color" in body:
            vals["color"] = clean_color(body["color"])
        if not vals:
            raise BadRequest("nothing to update", remediation="send " + ", ".join(sorted(allowed)))
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.db.begin() as c:
            if "name_key" in vals and fetch_one(c, select(gov_classifications.c.id).where(gov_classifications.c.name_key == vals["name_key"], gov_classifications.c.id != row["id"])):
                raise Conflict(f"a classification named '{vals['name']}' already exists")
            n = c.execute(update(gov_classifications).where(gov_classifications.c.id == row["id"], gov_classifications.c.etag == row["etag"]).values(**vals)).rowcount
        if n == 0:
            raise_precondition(self.resolve(row["id"])["etag"])
        self.audit(principal, "governance.classification_updated", resource_type="classification", resource_id=row["id"], details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "name_key"))})
        return self.get(principal, row["id"])

    def delete(self, principal: Principal, ref: str, if_match: str | None = None) -> dict[str, Any]:
        """Delete a custom label that has no live assignments (``governance.approve`` org-wide)."""
        self.org_approver(principal)
        row = self.resolve(ref)
        self.check_etag(if_match, row["etag"])
        if row["builtin"]:
            raise Conflict(f"'{row['name']}' is a built-in classification and cannot be deleted")
        n = self._counts([row["id"]]).get(row["id"], 0)
        if n:
            raise Conflict(f"classification '{row['name']}' still has {n} assignment(s)", remediation="remove or reassign them first")
        with self.db.begin() as c:
            c.execute(delete(gov_class_assignments).where(gov_class_assignments.c.class_id == row["id"]))   # only rejected rows are left
            c.execute(delete(gov_classifications).where(gov_classifications.c.id == row["id"]))
        self.audit(principal, "governance.classification_deleted", resource_type="classification", resource_id=row["id"], details={"name": row["name"]})
        return {"id": row["id"], "deleted": True}

    # ------------------------------------------------------------------------------------------------------------------------ assignments
    def _public_assignment(self, r: dict[str, Any], refs: dict[str, dict[str, Any]]) -> dict[str, Any]:
        return {"id": r["id"], "asset_id": r["asset_id"], "classification": refs.get(r["class_id"]), "source": r["source"], "status": r["status"], "confidence": r["confidence"],
                "evidence": r["evidence"] or {}, "comment": r["comment"], "created_by": r["created_by"], "created_at": r["created_at"], "reviewed_by": r["reviewed_by"],
                "reviewed_at": r["reviewed_at"], "review_comment": r["review_comment"]}

    def rows_for_assets(self, asset_ids: builtins.list[str], statuses: tuple[str, ...] = ("approved",)) -> dict[str, builtins.list[dict[str, Any]]]:
        """Assignment rows per asset id for the given statuses."""
        out: dict[str, builtins.list[dict[str, Any]]] = {i: [] for i in asset_ids}
        g = gov_class_assignments.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(asset_ids))):
                for r in fetch_all(c, select(gov_class_assignments).where(g.asset_id.in_(part), g.status.in_(statuses)).order_by(g.id)):
                    out[r["asset_id"]].append(r)
        return out

    def contained(self, table_ids: builtins.list[str]) -> dict[str, builtins.list[dict[str, Any]]]:
        """For each asset the labels carried (directly, approved) by its child columns: ``[{"classification": ClassRef, "columns": n}]``."""
        out: dict[str, dict[str, int]] = {i: {} for i in table_ids}
        g = gov_class_assignments.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(table_ids))):
                stmt = (select(assets.c.parent_id, g.class_id, func.count()).join(assets, assets.c.id == g.asset_id)
                        .where(assets.c.parent_id.in_(part), assets.c.type == "column", assets.c.status != "removed", g.status == "approved").group_by(assets.c.parent_id, g.class_id))
                for pid, cid, n in c.execute(stmt):
                    out[pid][cid] = int(n)
        refs = self.refs({cid for d in out.values() for cid in d})
        return {k: [{"classification": refs[cid], "columns": n} for cid, n in sorted(d.items(), key=lambda x: refs[x[0]]["name"]) if cid in refs] for k, d in out.items()}

    def for_asset(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """Everything classification knows about one asset: direct (approved), inherited (computed), suggestions, rejected, and labels contained in its columns."""
        row = self.asset(principal, asset_id)
        rows = self.rows_for_assets([row["id"]], ("approved", "suggested", "rejected"))[row["id"]]
        refs = self.refs({r["class_id"] for r in rows})
        shaped = {s: [self._public_assignment(r, refs) for r in rows if r["status"] == s] for s in ("approved", "suggested", "rejected")}
        inh = self.gov.propagation.inherited_for([row["id"]]).get(row["id"], [])
        return {"asset_id": row["id"], "direct": shaped["approved"], "inherited": self.gov.propagation.shape_many(principal, inh), "suggestions": shaped["suggested"],
                "rejected": shaped["rejected"], "contained": self.contained([row["id"]])[row["id"]]}

    def _four_eyes(self) -> bool:
        return bool(self.setting("governance.classification_four_eyes", False))

    def _may_approve(self, principal: Principal, workspace_id: str | None, created_by: str | None) -> tuple[bool, str | None]:
        """Can this principal approve a suggestion of ``created_by`` for an asset of ``workspace_id``? Returns ``(allowed, reason if not)``."""
        if self._four_eyes():
            if not principal.can("governance.approve", workspace_id=workspace_id):
                return False, "four-eyes approval is on (governance.classification_four_eyes): approving needs governance.approve"
            if created_by and created_by == principal.id and not self.setting("governance.allow_self_approval", False):
                return False, "four-eyes approval is on: the person who proposed a classification cannot approve it (governance.allow_self_approval is off)"
            return True, None
        return principal.can("governance.curate", workspace_id=workspace_id), "governance.curate is required"

    def assign(self, principal: Principal | None, asset_id: str, class_ref: Any, *, comment: str | None = None, authorized_row: dict[str, Any] | None = None,
               via_change: str | None = None) -> dict[str, Any]:
        """Assign a label to an asset (``governance.curate`` in its workspace). Result status is ``approved``, or ``suggested`` when four-eyes is on and the caller cannot approve."""
        if authorized_row is not None:
            row = authorized_row
        else:
            assert principal is not None
            row = self.curate_asset(principal, asset_id)
        cls = self.resolve(class_ref)
        if row["type"] == "workspace":
            raise BadRequest("classifications go on data assets, not on workspaces")
        comment = clean_text(comment, "comment", max_len=2000)
        g = gov_class_assignments.c
        now = now_iso()
        who = principal.id if principal else None
        approve_now = True
        if via_change is None and principal is not None and self._four_eyes():
            approve_now, _ = self._may_approve(principal, row["workspace_id"], who)
        with self.db.begin() as c:
            ex = fetch_one(c, select(gov_class_assignments).where(g.asset_id == row["id"], g.class_id == cls["id"]))
            previous = ex["status"] if ex else None
            if ex is not None and ex["status"] == "approved":
                raise Conflict(f"'{cls['name']}' is already assigned to '{row['name']}'", details={"assignment_id": ex["id"]})
            if ex is not None and ex["status"] == "suggested" and not approve_now:
                raise Conflict(f"'{cls['name']}' is already proposed for '{row['name']}' and waits for review", details={"assignment_id": ex["id"]})
            status = "approved" if approve_now else "suggested"
            vals = {"status": status, "comment": comment, "updated_at": now,
                    "reviewed_by": who if approve_now else None, "reviewed_at": now if approve_now else None, "review_comment": comment if approve_now else None}
            if ex is None:
                res = c.execute(insert(gov_class_assignments).values(class_id=cls["id"], asset_id=row["id"], source="manual", confidence=None, evidence={"change_request": via_change} if via_change else {},
                                                                     created_by=who, created_at=now, **vals))
                aid = int(res.inserted_primary_key[0])
            else:
                aid = ex["id"]
                c.execute(update(gov_class_assignments).where(g.id == aid).values(**vals))
            out = fetch_one(c, select(gov_class_assignments).where(g.id == aid))
        assert out is not None
        self.audit(principal, "governance.classification_assigned", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"],
                   details={"classification": cls["name"], "status": out["status"], "previous": previous, "change_id": via_change})
        if out["status"] == "approved":
            self._changed(row, cls, "approved", previous)
        else:
            self.emit("governance.review.requested", {"kind": "classification", "count": 1, "asset_id": row["id"], "classification": cls["name"], "source": "manual"}, workspace_id=row["workspace_id"], subject=row["id"])
        return self._public_assignment(out, {cls["id"]: self.ref_of(cls)})

    def _changed(self, row: dict[str, Any], cls: dict[str, Any], status: str, previous: str | None) -> None:
        self.emit("governance.classification.changed", {"asset_id": row["id"], "asset": row["name"], "classification_id": cls["id"], "classification": cls["name"], "status": status,
                                                         "previous": previous}, workspace_id=row["workspace_id"], subject=row["id"])

    def remove(self, principal: Principal | None, asset_id: str, class_ref: Any, *, authorized_row: dict[str, Any] | None = None) -> dict[str, Any]:
        """Remove a label from an asset (suggested and rejected rows too). Removing an approved label needs ``governance.approve`` when four-eyes is on."""
        if authorized_row is not None:
            row = authorized_row
        else:
            assert principal is not None
            row = self.curate_asset(principal, asset_id)
        cls = self.resolve(class_ref)
        g = gov_class_assignments.c
        with self.db.begin() as c:
            ex = fetch_one(c, select(gov_class_assignments).where(g.asset_id == row["id"], g.class_id == cls["id"]))
            if ex is None:
                raise NotFound(f"'{row['name']}' has no '{cls['name']}' classification")
            if ex["status"] == "approved" and principal is not None and authorized_row is None and self._four_eyes() and not principal.can("governance.approve", workspace_id=row["workspace_id"]):
                raise self.denied("removing an approved classification needs governance.approve while four-eyes approval is on", "ask an approver or submit a classification_change request")
            c.execute(delete(gov_class_assignments).where(g.id == ex["id"]))
        self.audit(principal, "governance.classification_removed", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"],
                   details={"classification": cls["name"], "status": ex["status"]})
        if ex["status"] == "approved":
            self._changed(row, cls, "removed", "approved")
        return {"asset_id": row["id"], "classification": self.ref_of(cls), "removed": True, "previous_status": ex["status"]}

    # ------------------------------------------------------------------------------------------------------------------------ listing assets of a label
    def assets_of(self, principal: Principal, class_ref: str, *, workspace_id: str | None = None, include_inherited: bool = True, type: str | None = None,
                  limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Assets that carry the label directly (approved) or, by default, by inheritance (proven only)."""
        self.org_reader(principal)
        cls = self.resolve(class_ref)
        g = gov_class_assignments.c
        found: dict[str, dict[str, Any]] = {}
        with self.db.connect() as c:
            for r in fetch_all(c, select(g.id, g.asset_id, g.confidence, g.status).where(g.class_id == cls["id"], g.status == "approved").limit(20000)):
                found[r["asset_id"]] = {"source": "direct", "status": "approved", "confidence": r["confidence"], "inherited": False, "precision": None, "assignment_id": r["id"]}
        if include_inherited:
            for item in self.gov.propagation.result().by_class.get(cls["id"], []):
                if item.proven and item.target_id not in found:
                    found[item.target_id] = {"source": "inherited", "status": "approved", "confidence": item.confidence, "inherited": True, "precision": item.precision,
                                             "assignment_id": item.assignment_id}
        ids = sorted(found)
        cur = decode_cursor(cursor)
        if cur:
            ids = [i for i in ids if i > cur[0]]
        limit = max(1, min(int(limit), 500))
        kinds = set(type.split(",")) if type else None
        out: builtins.list[dict[str, Any]] = []
        for part in chunks(ids, 500):
            stmt = self.intel.scope_filter(select(assets).where(assets.c.id.in_(part), assets.c.status != "removed"), principal, workspace_id).order_by(assets.c.id)
            with self.db.connect() as c:
                for r in fetch_all(c, stmt):
                    if kinds and r["type"] not in kinds:
                        continue
                    out.append({"asset": compact_asset(r), **found[r["id"]]})
            if len(out) > limit:
                break
        nxt = encode_cursor([out[limit - 1]["asset"]["id"]]) if len(out) > limit else None
        return out[:limit], nxt

    def classified_column_names(self, asset_id: str) -> set[str]:
        """Lower-case names of the columns of ``asset_id`` (or the column itself) that carry an approved or inherited label. Registered with the sampling service."""
        row = self.intel.graph.asset_rows([asset_id]).get(asset_id)
        if row is None:
            return set()
        cols = [row] if row["type"] == "column" else self._children(row["id"], "column")
        ids = [c["id"] for c in cols]
        direct = self.rows_for_assets(ids)
        inherited = self.gov.propagation.inherited_for(ids)
        return {c["name"].lower() for c in cols if direct.get(c["id"]) or inherited.get(c["id"])}

    def _children(self, parent_id: str, type_: str | None = None) -> builtins.list[dict[str, Any]]:
        stmt = select(assets).where(assets.c.parent_id == parent_id, assets.c.status != "removed")
        if type_:
            stmt = stmt.where(assets.c.type == type_)
        with self.db.connect() as c:
            return fetch_all(c, stmt.limit(5000))

    # ------------------------------------------------------------------------------------------------------------------------ exposure
    def exposure(self, principal: Principal, class_ref: str, *, depth: int | None = None, min_confidence: str | None = None, include_possible: bool = True,
                 types: str | None = None, workspace_id: str | None = None) -> dict[str, Any]:
        """Where does data labelled ``class_ref`` end up? See the module docstring of :mod:`propagation` for ``precision`` / ``proven``."""
        self.org_reader(principal)
        cls = self.resolve(class_ref)
        prop = self.gov.propagation
        if depth is None and min_confidence is None:
            res = prop.result()
        else:
            if min_confidence is not None and min_confidence not in flow.RANK:
                raise BadRequest(f"min_confidence must be one of {list(flow.RANK)}")
            if depth is not None and not 1 <= int(depth) <= 10:
                raise BadRequest("depth must be between 1 and 10")
            res = prop.compute(only_class=cls.get("id"), depth=depth, min_confidence=min_confidence)
        kinds = set(types.split(",")) if types else None
        items = [i for i in res.by_class.get(cls["id"], []) if i.proven or include_possible]
        src_pairs = [(aid, asg) for asg, cid, aid in res.sources if cid == cls["id"]]
        rows = prop.rows_for(items)
        rows.update(self.intel.graph.asset_rows([a for a, _ in src_pairs]))
        vis = {i for i, r in rows.items() if self.intel.can_see(principal, r["workspace_id"])}
        exposures = []
        for it in sorted(items, key=lambda i: (not i.proven, flow.RANK[i.floor], len(i.nodes), i.target_id)):
            trow = rows.get(it.target_id)
            if trow is None or it.target_id not in vis:
                continue
            if kinds and trow["type"] not in kinds:
                continue
            if workspace_id and trow["workspace_id"] != workspace_id:
                continue
            shaped = prop.shape(principal, it, {cls["id"]: self.ref_of(cls)}, rows)
            exposures.append({"asset": compact_asset(trow), "kind": trow["type"], "from_asset": shaped["evidence"]["from_asset"], "precision": it.precision, "proven": it.proven,
                              "confidence": it.confidence, "confidence_floor": it.floor, "path": shaped["evidence"]["path"],
                              **({"contains": shaped["evidence"]["contains"]} if it.via else {})})
        by_type: dict[str, int] = {}
        for e in exposures:
            by_type[e["kind"]] = by_type.get(e["kind"], 0) + 1
        products = self._products_receiving([e["asset"]["id"] for e in exposures])
        notes = ["Inherited exposure is computed from lineage; lineage is incomplete wherever gaps exist. A missing exposure is not proof that none exists."]
        if any(not e["proven"] for e in exposures):
            notes.append("Entries with proven=false are possible exposures: the label sits on a column and only table-level lineage reaches them, so it is not known that the column is used.")
        if res.truncated:
            notes.append("The lineage walk hit a configured limit (depth / node cap / number of classified assets): the list may be incomplete.")
        return {"classification": self.ref_of(cls),
                "sources": [{"asset": compact_asset(rows[a]) if a in vis and a in rows else dict(HIDDEN), "assignment_id": asg} for a, asg in src_pairs],
                "exposures": exposures,
                "summary": {"by_type": dict(sorted(by_type.items())), "reports": by_type.get("report", 0) + by_type.get("dashboard", 0), "visuals": by_type.get("visual", 0),
                            "semantic_models": by_type.get("semantic_model", 0), "products": len(products), "proven": sum(1 for e in exposures if e["proven"]), "possible": sum(1 for e in exposures if not e["proven"])},
                "data_products": products, "truncated": bool(res.truncated), "notes": notes}

    def _products_receiving(self, asset_ids: builtins.list[str]) -> builtins.list[dict[str, Any]]:
        if not asset_ids:
            return []
        out: dict[str, dict[str, Any]] = {}
        m, p = gov_product_members.c, gov_products.c
        with self.db.connect() as c:
            for part in chunks(asset_ids):
                for r in c.execute(select(m.asset_id, p.id, p.name, p.status).join(gov_products, gov_products.c.id == m.product_id).where(m.asset_id.in_(part))):
                    mm = r._mapping
                    out.setdefault(mm["id"], {"product": {"id": mm["id"], "name": mm["name"], "status": mm["status"]}, "via_assets": []})["via_assets"].append(mm["asset_id"])
        return sorted(out.values(), key=lambda x: x["product"]["name"])

    # ------------------------------------------------------------------------------------------------------------------------ ingestion of suggestions
    def ingest(self, rows: builtins.list[dict[str, Any]]) -> dict[str, int]:
        """Turn the ``attrs.suggested_classifications`` of ``rows`` (column assets) into ``suggested`` auto assignments. Idempotent: an existing suggestion is refreshed, an approved,
        rejected or manual assignment is never touched, a suggestion the detector no longer makes is withdrawn. Returns counts (``created``, ``updated``, ``withdrawn``, ``skipped``)."""
        min_conf = float(self.setting("governance.suggestion_min_confidence", 0.5))
        stats = {"examined": 0, "created": 0, "updated": 0, "withdrawn": 0, "skipped": 0, "unmapped": 0}
        catalog = {name_key(n): self.row_or_none(n) for n, *_ in BUILTINS}
        created_by_ws: dict[str | None, int] = {}
        g = gov_class_assignments.c
        for part in chunks([r for r in rows if r["type"] == "column"], 200):
            ids = [r["id"] for r in part]
            existing = self.rows_for_assets(ids, ("suggested", "approved", "rejected"))
            now = now_iso()
            for r in part:
                stats["examined"] += 1
                mapped: dict[str, dict[str, Any]] = {}
                for s in (r["attrs"] or {}).get("suggested_classifications") or []:
                    if not isinstance(s, dict) or not s.get("classification"):
                        continue
                    target = DETECTOR_MAP.get(str(s["classification"]))
                    cls = catalog.get(name_key(target)) if target else None
                    if cls is None:
                        stats["unmapped"] += 1
                        continue
                    conf = float(s.get("confidence") or 0)
                    if conf < min_conf:
                        continue
                    cur = mapped.get(cls["id"])
                    if cur is None or conf > cur["confidence"]:
                        mapped[cls["id"]] = {"confidence": round(conf, 3), "evidence": self._evidence(s)}
                have = {e["class_id"]: e for e in existing[r["id"]]}
                for cid, m in mapped.items():
                    ex = have.get(cid)
                    if ex is None:
                        try:
                            with self.db.begin() as c:
                                c.execute(insert(gov_class_assignments).values(class_id=cid, asset_id=r["id"], source="auto", status="suggested", confidence=m["confidence"], evidence=m["evidence"],
                                                                               created_by=None, created_at=now, updated_at=now))
                            stats["created"] += 1
                            created_by_ws[r["workspace_id"]] = created_by_ws.get(r["workspace_id"], 0) + 1
                        except IntegrityError:
                            stats["skipped"] += 1
                    elif ex["status"] == "suggested" and ex["source"] == "auto":
                        if ex["confidence"] != m["confidence"] or ex["evidence"] != m["evidence"]:
                            with self.db.begin() as c:
                                c.execute(update(gov_class_assignments).where(g.id == ex["id"]).values(confidence=m["confidence"], evidence=m["evidence"], updated_at=now))
                            stats["updated"] += 1
                    else:
                        stats["skipped"] += 1
                for cid, ex in have.items():
                    if cid not in mapped and ex["status"] == "suggested" and ex["source"] == "auto":
                        with self.db.begin() as c:
                            c.execute(delete(gov_class_assignments).where(g.id == ex["id"], g.status == "suggested"))
                        stats["withdrawn"] += 1
        for ws, n in created_by_ws.items():
            self.emit("governance.review.requested", {"kind": "classification", "count": n, "source": "discovery"}, workspace_id=ws)
        return stats

    @staticmethod
    def _evidence(s: dict[str, Any]) -> dict[str, Any]:
        """The evidence stored with a suggestion: rule names and evidence kinds only (the detector never returns values; this keeps it that way)."""
        kinds = s.get("evidences") or ([s["evidence"]] if s.get("evidence") else [])
        return {"rule": str(s.get("rule") or "")[:200], "detector_label": str(s["classification"])[:64], "evidence_kinds": [str(k)[:32] for k in kinds][:5],
                "category": str(s.get("category") or "")[:32]}

    def scan(self, principal: Principal, workspace_id: str | None = None, scan_id: str | None = None) -> dict[str, Any]:
        """``POST /governance/classifications/scan``: ingest the suggestions of the visible column assets now (and propose glossary links)."""
        if workspace_id:
            principal.require("governance.curate", workspace_id=workspace_id)
        else:
            principal.require("governance.curate")
        self.intel.scope(principal)
        out = self.scan_assets(workspace_id=workspace_id, scan_id=scan_id, scope=lambda stmt: self.intel.scope_filter(stmt, principal, workspace_id))
        self.audit(principal, "governance.classification_scanned", resource_type="scan", resource_id=scan_id or "on-demand", workspace_id=workspace_id,
                   details={"suggested": out["classification"]["created"], "term_links": out["term_links"]["suggested"], "on_demand": True})
        return out

    def scan_assets(self, *, workspace_id: str | None = None, scan_id: str | None = None, source_id: str | None = None, scope: Any = None, max_assets: int = 200_000) -> dict[str, Any]:
        """Page through column assets with suggestions (and the assets whose names may match glossary terms) and ingest them. ``scope`` narrows the statement (visibility)."""
        t = assets.c
        total = {"examined": 0, "created": 0, "updated": 0, "withdrawn": 0, "skipped": 0, "unmapped": 0}
        terms = {"examined": 0, "suggested": 0}
        last = ""
        seen = 0
        suggest_terms = bool(self.setting("governance.suggest_term_links", True))
        while seen < max_assets:
            stmt = select(assets).where(assets.c.status != "removed", assets.c.id > last)
            if scan_id:
                stmt = stmt.where(t.last_seen_scan == scan_id)
            if source_id:
                stmt = stmt.where(t.source_id == source_id)
            if scope is not None:
                stmt = scope(stmt)
            elif workspace_id:
                stmt = stmt.where(t.workspace_id == workspace_id)
            with self.db.connect() as c:
                page = fetch_all(c, stmt.order_by(t.id).limit(500))
            if not page:
                break
            last = page[-1]["id"]
            seen += len(page)
            st = self.ingest([r for r in page if r["type"] == "column"])
            for k in total:
                total[k] += st.get(k, 0)
            if suggest_terms:
                ts = self.gov.glossary.suggest_for_assets(page)
                terms["examined"] += ts["examined"]
                terms["suggested"] += ts["suggested"]
                if ts["suggested"]:
                    self.emit("governance.review.requested", {"kind": "term_link", "count": ts["suggested"], "source": "glossary-suggestions"}, workspace_id=workspace_id)
        return {"assets_examined": seen, "classification": total, "term_links": terms}

    # ------------------------------------------------------------------------------------------------------------------------ review queue
    def _queue_scope(self, principal: Principal, workspace_id: str | None) -> Any:
        return self.scope_ids(principal, workspace_id)

    def queue(self, principal: Principal, *, kind: str | None = None, asset_id: str | None = None, workspace_id: str | None = None, classification: str | None = None,
              term_id: str | None = None, min_confidence: float | None = None, q: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Pending suggestions (classification and glossary links), oldest first. Items are limited to assets the caller can see."""
        self.intel.scope(principal)
        if kind and kind not in REVIEW_KINDS:
            raise BadRequest(f"kind must be one of {list(REVIEW_KINDS)}")
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        visible = self._queue_scope(principal, workspace_id)
        rows: builtins.list[tuple[str, int, dict[str, Any]]] = []
        qlike = None
        if q:
            from olcf.intelligence.governance.common import like_escape

            qlike = like_escape(q.lower())
        if kind in (None, "classification"):
            g = gov_class_assignments.c
            stmt = select(gov_class_assignments).where(g.status == "suggested", g.asset_id.in_(visible))
            if asset_id:
                stmt = stmt.where(g.asset_id == asset_id)
            if classification:
                cls = self.row_or_none(classification)
                stmt = stmt.where(g.class_id == (cls["id"] if cls else ""))
            if min_confidence is not None:
                stmt = stmt.where(g.confidence >= min_confidence)
            if qlike:
                stmt = stmt.where(g.asset_id.in_(select(assets.c.id).where(func.lower(assets.c.name).like(qlike, escape="\\"))))
            if cur:
                stmt = stmt.where(or_(g.created_at > cur[0], and_(g.created_at == cur[0], g.id > cur[2]) if cur[1] == 0 else false()))
            with self.db.connect() as c:
                rows += [("classification", r["id"], r) for r in fetch_all(c, stmt.order_by(g.created_at, g.id).limit(limit + 1))]
        if kind in (None, "term_link") and not classification:
            t = gov_term_links.c
            stmt = select(gov_term_links).where(t.status == "pending", t.subject_type == "asset", t.subject_id.in_(visible))
            if asset_id:
                stmt = stmt.where(t.subject_id == asset_id)
            if term_id:
                stmt = stmt.where(t.term_id == term_id)
            if min_confidence is not None:
                stmt = stmt.where(t.confidence >= min_confidence)
            if qlike:
                stmt = stmt.where(t.subject_id.in_(select(assets.c.id).where(func.lower(assets.c.name).like(qlike, escape="\\"))))
            if cur:
                stmt = stmt.where(or_(t.created_at > cur[0], and_(t.created_at == cur[0], t.id > cur[2]) if cur[1] == 1 else (t.created_at == cur[0] if cur[1] < 1 else false())))
            with self.db.connect() as c:
                rows += [("term_link", r["id"], r) for r in fetch_all(c, stmt.order_by(t.created_at, t.id).limit(limit + 1))]
        rank = {"classification": 0, "term_link": 1}
        rows.sort(key=lambda x: (x[2]["created_at"], rank[x[0]], x[1]))
        page = rows[:limit]
        nxt = None
        if len(rows) > limit:
            last = page[-1]
            nxt = encode_cursor([last[2]["created_at"], rank[last[0]], last[1]])
        return self._shape_queue(principal, page), nxt

    def _shape_queue(self, principal: Principal, page: builtins.list[tuple[str, int, dict[str, Any]]]) -> builtins.list[dict[str, Any]]:
        arows = self.intel.graph.asset_rows([r["asset_id"] if k == "classification" else r["subject_id"] for k, _, r in page])
        refs = self.refs({r["class_id"] for k, _, r in page if k == "classification"})
        terms = self.gov.glossary.refs([r["term_id"] for k, _, r in page if k == "term_link"])
        out = []
        for k, n, r in page:
            aid = r["asset_id"] if k == "classification" else r["subject_id"]
            arow = arows.get(aid)
            if arow is None:
                continue
            item: dict[str, Any] = {"id": _item_id(k, n), "kind": k, "asset": compact_asset(arow), "source": r["source"], "confidence": r["confidence"], "evidence": r["evidence"] or {},
                                    "comment": r["comment"], "created_by": r["created_by"], "created_at": r["created_at"]}
            if k == "classification":
                item["classification"] = refs.get(r["class_id"])
                item["requires"] = "governance.approve" if self._four_eyes() else "governance.curate"
            else:
                item["term"] = terms.get(r["term_id"])
                item["requires"] = "governance.curate"
            out.append(item)
        return out

    def queue_counts(self, principal: Principal, workspace_id: str | None = None) -> dict[str, int]:
        """Pending items by kind (visible assets only) plus pending change requests."""
        visible = self._queue_scope(principal, workspace_id)
        with self.db.connect() as c:
            cls_n = int(c.execute(select(func.count()).select_from(gov_class_assignments).where(gov_class_assignments.c.status == "suggested", gov_class_assignments.c.asset_id.in_(visible))).scalar() or 0)
            tl_n = int(c.execute(select(func.count()).select_from(gov_term_links).where(gov_term_links.c.status == "pending", gov_term_links.c.subject_type == "asset",
                                                                                          gov_term_links.c.subject_id.in_(visible))).scalar() or 0)
        chg = self.gov.workflow.pending_count(principal, workspace_id)
        return {"total": cls_n + tl_n, "classification": cls_n, "term_link": tl_n, "change_requests": chg}

    def review(self, principal: Principal, item_id: str, action: str, comment: str | None = None) -> dict[str, Any]:
        """Approve or reject one queue item (see the module docstring for who may)."""
        if action not in ("approve", "reject"):
            raise BadRequest("action must be approve or reject")
        kind, n = parse_item_id(item_id)
        comment = clean_text(comment, "comment", max_len=2000)
        now = now_iso()
        if kind == "classification":
            g = gov_class_assignments.c
            with self.db.connect() as c:
                row = fetch_one(c, select(gov_class_assignments).where(g.id == n))
            if row is None:
                raise NotFound(f"review item '{item_id}' does not exist")
            arow = self.asset(principal, row["asset_id"])
            # permissions first: a caller who may not decide must not learn whether the item was already decided
            if action == "approve":
                ok, why = self._may_approve(principal, arow["workspace_id"], row["created_by"])
                if not ok:
                    if self._four_eyes():
                        raise self.denied(why or "not allowed", "ask a different approver (governance.approve)")
                    principal.require("governance.curate", workspace_id=arow["workspace_id"])
            else:
                principal.require("governance.curate", workspace_id=arow["workspace_id"])
            if row["status"] != "suggested":
                raise Conflict(f"review item '{item_id}' was already decided ({row['status']})")
            status = "approved" if action == "approve" else "rejected"
            with self.db.begin() as c:
                done = c.execute(update(gov_class_assignments).where(g.id == n, g.status == "suggested").values(status=status, reviewed_by=principal.id, reviewed_at=now, review_comment=comment,
                                                                                                                updated_at=now)).rowcount
            if done == 0:
                raise Conflict(f"review item '{item_id}' was decided concurrently")
            cls = self.resolve(row["class_id"])
            self.audit(principal, "governance.classification_reviewed", resource_type="asset", resource_id=arow["id"], workspace_id=arow["workspace_id"],
                       details={"item": item_id, "decision": status, "classification": cls["name"], "source": row["source"], "confidence": row["confidence"]})
            self.emit("governance.review.decided", {"item": item_id, "kind": "classification", "decision": status, "asset_id": arow["id"], "classification": cls["name"]},
                      workspace_id=arow["workspace_id"], subject=arow["id"])
            if status == "approved":
                self._changed(arow, cls, "approved", "suggested")
            return {"id": item_id, "kind": kind, "decision": status, "asset_id": arow["id"], "classification": self.ref_of(cls)}
        t = gov_term_links.c
        with self.db.connect() as c:
            lrow = fetch_one(c, select(gov_term_links).where(t.id == n))
        if lrow is None or lrow["subject_type"] != "asset":
            raise NotFound(f"review item '{item_id}' does not exist")
        arow = self.asset(principal, lrow["subject_id"])
        principal.require("governance.curate", workspace_id=arow["workspace_id"])
        if lrow["status"] != "pending":
            raise Conflict(f"review item '{item_id}' was already decided ({lrow['status']})")
        status = "approved" if action == "approve" else "rejected"
        with self.db.begin() as c:
            done = c.execute(update(gov_term_links).where(t.id == n, t.status == "pending").values(status=status, reviewed_by=principal.id, reviewed_at=now, review_comment=comment)).rowcount
        if done == 0:
            raise Conflict(f"review item '{item_id}' was decided concurrently")
        term = self.gov.glossary.refs([lrow["term_id"]]).get(lrow["term_id"])
        self.audit(principal, "governance.term_linked" if status == "approved" else "governance.term_unlinked", resource_type="asset", resource_id=arow["id"], workspace_id=arow["workspace_id"],
                   details={"item": item_id, "decision": status, "term_id": lrow["term_id"], "source": lrow["source"]})
        self.emit("governance.review.decided", {"item": item_id, "kind": "term_link", "decision": status, "asset_id": arow["id"], "term": (term or {}).get("name")},
                  workspace_id=arow["workspace_id"], subject=arow["id"])
        return {"id": item_id, "kind": kind, "decision": status, "asset_id": arow["id"], "term": term}

    def bulk(self, principal: Principal, action: str, ids: Any, comment: str | None = None) -> dict[str, Any]:
        """Approve or reject up to 500 items; each is decided on its own (one failure does not stop the rest)."""
        if action not in ("approve", "reject"):
            raise BadRequest("action must be approve or reject")
        if not isinstance(ids, list) or not ids or not all(isinstance(i, str) for i in ids):
            raise BadRequest("ids must be a non-empty list of review item ids")
        if len(ids) > MAX_BULK:
            raise BadRequest(f"at most {MAX_BULK} items per bulk request")
        results = []
        ok = failed = 0
        for item in dict.fromkeys(ids):
            try:
                r = self.review(principal, item, action, comment)
                results.append({"id": item, "ok": True, "decision": r["decision"]})
                ok += 1
            except (OlcfError, Forbidden) as exc:
                results.append({"id": item, "ok": False, "error": {"code": exc.code, "message": exc.message}})
                failed += 1
        return {"results": results, "ok": ok, "failed": failed}


__all__ = ["BUILTINS", "DETECTOR_MAP", "ClassificationManager", "parse_item_id"]
