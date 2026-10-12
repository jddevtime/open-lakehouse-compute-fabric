# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Owners, stewards, custodians and contacts - and how ownership is inherited.

A subject (an asset, a domain, a data product or a glossary term) has any number of *direct* owner rows. Each row names an OLCF user (``kind = "user"``, stored with the principal
id, so a rename does not break it) or an external contact (``kind = "external"``: a name and usually an e-mail address). The role is one of ``owner``, ``steward``, ``custodian``,
``contact``.

**Inheritance (assets only).** The *effective* owners of an asset are computed **per role**: for each role, the nearest asset on the chain *asset -> parent -> grandparent ...*
(column -> table -> lakehouse / schema -> workspace asset) that has at least one holder of that role supplies the holders of that role; ``inherited_from`` names that ancestor
and its distance. A column with its own steward but no owner therefore has its own steward and the table's owner. Nothing is copied: change the table owner and every column
that inherits it changes with it. Domains, products and terms have no parent chain in this sense (a term's or domain's ancestors are a different relation) and report direct owners only.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import and_, delete, func, insert, select

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one
from olcf.intelligence.governance.common import (
    OWNER_ROLES,
    SUBJECT_TYPES,
    Base,
    chunks,
    clean_name,
    clean_text,
)
from olcf.intelligence.governance.schema import gov_domains, gov_owners, gov_products, gov_terms
from olcf.security.rbac import Principal

MAX_OWNERS_PER_SUBJECT = 50
MAX_CHAIN = 8


class OwnershipManager(Base):
    """Direct and effective owners; every method that changes data checks the caller's rights through :meth:`authorize`."""

    # ------------------------------------------------------------------------------------------------------------------------ parsing
    def find_user(self, ref: str) -> dict[str, Any] | None:
        """The OLCF principal that ``ref`` names (principal id, user name or e-mail, case-insensitive); ``None`` if there is none."""
        r = ref.strip()
        if not r:
            return None
        t = S.principals.c
        with self.db.connect() as c:
            row = fetch_one(c, select(S.principals).where(t.id == r, t.is_active.is_(True)))
            if row is None:
                row = fetch_one(c, select(S.principals).where(func.lower(t.name) == r.lower(), t.is_active.is_(True)))
            if row is None:
                rows = fetch_all(c, select(S.principals).where(func.lower(t.email) == r.lower(), t.is_active.is_(True)).limit(2))
                row = rows[0] if len(rows) == 1 else None
        return row

    def parse(self, spec: Any) -> dict[str, Any]:
        """Validate one owner request body and resolve it into the row values (``role``, ``kind``, ``user_id``, ``name``, ``email``, ``notes``, ``owner_key``)."""
        if not isinstance(spec, dict):
            raise BadRequest("an owner must be an object like {\"role\": \"owner\", \"user\": \"alice\"}")
        extra = set(spec) - {"role", "user", "name", "email", "notes"}
        if extra:
            raise BadRequest(f"unknown owner field(s) {sorted(extra)}", details={"allowed": ["role", "user", "name", "email", "notes"]})
        role = spec.get("role", "owner")
        if role not in OWNER_ROLES:
            raise BadRequest(f"role must be one of {list(OWNER_ROLES)}")
        notes = clean_text(spec.get("notes"), "notes", max_len=500)
        if spec.get("user"):
            if spec.get("name") or spec.get("email"):
                raise BadRequest("send either 'user' (an OLCF user) or 'name' / 'email' (an external contact), not both")
            if not isinstance(spec["user"], str):
                raise BadRequest("user must be a string (user id, name or e-mail)")
            u = self.find_user(spec["user"])
            if u is None:
                raise BadRequest(f"no active OLCF user matches '{spec['user']}'", remediation="use the user id, user name or e-mail, or send {\"name\", \"email\"} for an external contact")
            return {"role": role, "kind": "user", "user_id": u["id"], "name": u["display_name"] or u["name"], "email": u.get("email"), "notes": notes, "owner_key": f"u:{u['id']}"}
        name = spec.get("name")
        email = spec.get("email")
        if email is not None and (not isinstance(email, str) or "@" not in email or len(email) > 255):
            raise BadRequest("email must look like an e-mail address")
        if not name and email:
            name = email
        name = clean_name(name, "name of the external contact")
        key = f"e:{email.strip().lower()}" if email else f"n:{name.lower()}"
        return {"role": role, "kind": "external", "user_id": None, "name": name, "email": email.strip() if email else None, "notes": notes, "owner_key": key}

    # ------------------------------------------------------------------------------------------------------------------------ authorisation and existence
    def authorize(self, principal: Principal, subject_type: str, subject_id: str) -> dict[str, Any]:
        """Check the subject exists (and, for assets, is visible) and that the caller may change its owners; returns ``{"workspace_id", "name"}``."""
        if subject_type not in SUBJECT_TYPES:
            raise BadRequest(f"subject_type must be one of {list(SUBJECT_TYPES)}")
        if subject_type == "asset":
            row = self.curate_asset(principal, subject_id)
            return {"workspace_id": row["workspace_id"], "name": row["name"], "id": row["id"]}
        self.org_curator(principal)
        table = {"domain": gov_domains, "product": gov_products, "term": gov_terms}[subject_type]
        with self.db.connect() as c:
            found = fetch_one(c, select(table.c.id, table.c.name).where(table.c.id == subject_id))
        if found is None:
            raise NotFound(f"{subject_type} '{subject_id}' does not exist")
        return {"workspace_id": None, "name": found["name"], "id": found["id"]}

    def authorize_read(self, principal: Principal, subject_type: str, subject_id: str) -> str:
        """Read access to a subject's owners (asset visibility or org-level read); returns the canonical subject id."""
        if subject_type == "asset":
            return self.asset(principal, subject_id)["id"]
        self.org_reader(principal)
        table = {"domain": gov_domains, "product": gov_products, "term": gov_terms}[subject_type]
        with self.db.connect() as c:
            if fetch_one(c, select(table.c.id).where(table.c.id == subject_id)) is None:
                raise NotFound(f"{subject_type} '{subject_id}' does not exist")
        return subject_id

    # ------------------------------------------------------------------------------------------------------------------------ reads
    @staticmethod
    def public(row: dict[str, Any], inherited_from: dict[str, Any] | None = None) -> dict[str, Any]:
        """The API form of an owner row."""
        return {"id": row["id"], "role": row["role"], "kind": row["kind"], "user_id": row["user_id"], "name": row["name"], "email": row["email"], "notes": row["notes"],
                "inherited_from": inherited_from, "created_at": row["created_at"]}

    def direct(self, subject_type: str, ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Direct owners per subject id (rows ordered by role then name)."""
        out: dict[str, list[dict[str, Any]]] = {i: [] for i in ids}
        t = gov_owners.c
        with self.db.connect() as c:
            for part in chunks(ids):
                for r in fetch_all(c, select(gov_owners).where(t.subject_type == subject_type, t.subject_id.in_(part)).order_by(t.id)):
                    out[r["subject_id"]].append(r)
        order = {r: i for i, r in enumerate(OWNER_ROLES)}
        for rows in out.values():
            rows.sort(key=lambda r: (order.get(r["role"], 9), r["name"].lower(), r["id"]))
        return out

    def chains(self, ids: list[str], depth: int = MAX_CHAIN) -> tuple[dict[str, list[str]], dict[str, dict[str, Any]]]:
        """For each asset id the chain ``[id, parent, grandparent ...]`` (at most ``depth`` levels) and every asset row met on the way."""
        rows: dict[str, dict[str, Any]] = {}
        result: dict[str, list[str]] = {i: [i] for i in ids}
        tips = {i: i for i in ids}
        for _ in range(depth + 1):
            need = [t for t in set(tips.values()) if t not in rows]
            if need:
                rows.update(self.intel.graph.asset_rows(need))
            nxt: dict[str, str] = {}
            for leaf, tip in tips.items():
                p = (rows.get(tip) or {}).get("parent_id")
                if p and p not in result[leaf]:
                    result[leaf].append(p)
                    nxt[leaf] = p
            tips = nxt
            if not tips:
                break
        return result, rows

    def effective(self, asset_ids: list[str], principal: Principal | None = None) -> dict[str, list[dict[str, Any]]]:
        """Effective owners per asset (see the module docstring); ``principal`` hides the names of ancestors it cannot see."""
        ids = list(dict.fromkeys(asset_ids))
        if not ids:
            return {}
        chains, rows = self.chains(ids)
        every = sorted({a for ch in chains.values() for a in ch})
        direct = self.direct("asset", every)
        out: dict[str, list[dict[str, Any]]] = {}
        order = {r: i for i, r in enumerate(OWNER_ROLES)}
        for aid in ids:
            items: list[dict[str, Any]] = []
            for role in OWNER_ROLES:
                for dist, anc in enumerate(chains[aid]):
                    holders = [r for r in direct.get(anc, []) if r["role"] == role]
                    if not holders:
                        continue
                    src: dict[str, Any] | None = None
                    if dist > 0:
                        arow = rows.get(anc)
                        if arow is not None and (principal is None or self.intel.can_see(principal, arow["workspace_id"])):
                            src = {"asset_id": anc, "type": arow["type"], "name": arow["name"], "distance": dist}
                        else:
                            src = {"asset_id": None, "hidden": True, "distance": dist}
                    items.extend(self.public(r, src) for r in holders)
                    break
            items.sort(key=lambda o: (order.get(o["role"], 9), o["name"].lower()))
            out[aid] = items
        return out

    def owners_of_subject(self, principal: Principal, subject_type: str, subject_id: str) -> dict[str, Any]:
        """``{"subject_type", "subject_id", "direct", "effective"}`` for the owners routes."""
        sid = self.authorize_read(principal, subject_type, subject_id)
        direct = [self.public(r) for r in self.direct(subject_type, [sid])[sid]]
        effective = self.effective([sid], principal)[sid] if subject_type == "asset" else list(direct)
        return {"subject_type": subject_type, "subject_id": sid, "direct": direct, "effective": effective}

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def _insert(self, conn: Any, subject_type: str, subject_id: str, parsed: dict[str, Any], by: str | None) -> dict[str, Any]:
        t = gov_owners.c
        exists = fetch_one(conn, select(gov_owners).where(t.subject_type == subject_type, t.subject_id == subject_id, t.role == parsed["role"], t.owner_key == parsed["owner_key"]))
        if exists is not None:
            raise Conflict(f"{parsed['name']} is already {parsed['role']} of this {subject_type}", details={"owner_id": exists["id"]})
        n = int(conn.execute(select(func.count()).select_from(gov_owners).where(t.subject_type == subject_type, t.subject_id == subject_id)).scalar() or 0)
        if n >= MAX_OWNERS_PER_SUBJECT:
            raise BadRequest(f"a {subject_type} can have at most {MAX_OWNERS_PER_SUBJECT} owner entries")
        vals = {k: parsed[k] for k in ("role", "owner_key", "kind", "user_id", "name", "email", "notes")}
        res = conn.execute(insert(gov_owners).values(subject_type=subject_type, subject_id=subject_id, created_by=by, created_at=self._now(), **vals))
        return {"id": int(res.inserted_primary_key[0]), **vals, "created_at": self._now()}

    @staticmethod
    def _now() -> str:
        from olcf.core.timeutil import now_iso

        return now_iso()

    def add(self, principal: Principal | None, subject_type: str, subject_id: str, spec: Any, *, authorized: bool = False) -> dict[str, Any]:
        """Add one direct owner. ``authorized=True`` skips the rights check (the caller already proved them, for example by approving a change request)."""
        info = {"workspace_id": None, "name": subject_id}
        if not authorized:
            assert principal is not None
            info = self.authorize(principal, subject_type, subject_id)
        parsed = self.parse(spec)
        with self.db.begin() as c:
            row = self._insert(c, subject_type, subject_id, parsed, principal.id if principal else None)
        self.audit(principal, "governance.owner_added", resource_type=subject_type, resource_id=subject_id, workspace_id=info["workspace_id"],
                   details={"role": parsed["role"], "owner": parsed["name"], "kind": parsed["kind"]})
        return self.public({**row, "kind": parsed["kind"]})

    def replace(self, principal: Principal, subject_type: str, subject_id: str, specs: Any) -> dict[str, Any]:
        """Replace the direct owners of a subject with ``specs`` (an empty list removes them all)."""
        info = self.authorize(principal, subject_type, subject_id)
        if not isinstance(specs, list) or len(specs) > MAX_OWNERS_PER_SUBJECT:
            raise BadRequest(f"owners must be a list of at most {MAX_OWNERS_PER_SUBJECT} owner objects")
        parsed = [self.parse(s) for s in specs]
        wanted = {(p["role"], p["owner_key"]): p for p in parsed}
        t = gov_owners.c
        added: list[str] = []
        removed: list[str] = []
        with self.db.begin() as c:
            current = fetch_all(c, select(gov_owners).where(t.subject_type == subject_type, t.subject_id == subject_id))
            have = {(r["role"], r["owner_key"]): r for r in current}
            for key, r in have.items():
                if key not in wanted:
                    c.execute(delete(gov_owners).where(t.id == r["id"]))
                    removed.append(f"{r['role']}:{r['name']}")
            for key, p in wanted.items():
                if key not in have:
                    self._insert(c, subject_type, subject_id, p, principal.id)
                    added.append(f"{p['role']}:{p['name']}")
        if added:
            self.audit(principal, "governance.owner_added", resource_type=subject_type, resource_id=subject_id, workspace_id=info["workspace_id"], details={"owners": added})
        if removed:
            self.audit(principal, "governance.owner_removed", resource_type=subject_type, resource_id=subject_id, workspace_id=info["workspace_id"], details={"owners": removed})
        return self.owners_of_subject(principal, subject_type, subject_id)

    def remove(self, principal: Principal | None, subject_type: str, subject_id: str, owner_id: int, *, authorized: bool = False) -> None:
        """Remove one direct owner row of the subject."""
        info = {"workspace_id": None, "name": subject_id}
        if not authorized:
            assert principal is not None
            info = self.authorize(principal, subject_type, subject_id)
        t = gov_owners.c
        with self.db.begin() as c:
            row = fetch_one(c, select(gov_owners).where(and_(t.id == owner_id, t.subject_type == subject_type, t.subject_id == subject_id)))
            if row is None:
                raise NotFound(f"owner entry {owner_id} does not exist on this {subject_type}")
            c.execute(delete(gov_owners).where(t.id == owner_id))
        self.audit(principal, "governance.owner_removed", resource_type=subject_type, resource_id=subject_id, workspace_id=info["workspace_id"],
                   details={"role": row["role"], "owner": row["name"]})

    def remove_matching(self, subject_type: str, subject_id: str, selector: Any) -> int:
        """Remove direct owners named by ``selector`` (an owner id, or ``{"role", "user"|"email"|"name"}``); used by approved ownership changes. Returns how many went."""
        t = gov_owners.c
        with self.db.begin() as c:
            if isinstance(selector, int):
                return int(c.execute(delete(gov_owners).where(t.id == selector, t.subject_type == subject_type, t.subject_id == subject_id)).rowcount)
            parsed = self.parse(selector)
            return int(c.execute(delete(gov_owners).where(t.subject_type == subject_type, t.subject_id == subject_id, t.role == parsed["role"], t.owner_key == parsed["owner_key"])).rowcount)

    # ------------------------------------------------------------------------------------------------------------------------ lookups used by filters
    def asset_ids_for(self, who: str, *, limit: int = 20000) -> set[str]:
        """Assets whose *effective* owners (any role) include ``who`` (see :meth:`subjects_for`): the assets that list ``who`` directly, and for each role the assets below them
        that inherit that role (a descendant with a holder of its own for that role does not inherit it)."""
        w = who.strip()
        if not w:
            return set()
        from sqlalchemy import or_

        t = gov_owners.c
        conds = [func.lower(t.name) == w.lower(), func.lower(t.email) == w.lower()]
        u = self.find_user(w)
        if u is not None:
            conds.append(t.user_id == u["id"])
        by_role: dict[str, set[str]] = {}
        with self.db.connect() as c:
            for r in fetch_all(c, select(t.subject_id, t.role).where(t.subject_type == "asset", or_(*conds)).limit(limit)):
                by_role.setdefault(r["role"], set()).add(r["subject_id"])
            out: set[str] = set()
            for role, seeds in by_role.items():
                holders = {r[0] for r in c.execute(select(t.subject_id).where(t.subject_type == "asset", t.role == role))}
                out |= self.with_descendants(seeds, blocked=holders - seeds, limit=limit)
        return out

    def subjects_for(self, subject_type: str, who: str, *, limit: int = 20000) -> set[str]:
        """Ids of the subjects of ``subject_type`` that have ``who`` (principal id, user name, e-mail or the name of an external contact) as a *direct* owner of any role."""
        w = who.strip()
        if not w:
            return set()
        from sqlalchemy import or_

        t = gov_owners.c
        conds = [func.lower(t.name) == w.lower(), func.lower(t.email) == w.lower()]
        u = self.find_user(w)
        if u is not None:
            conds.append(t.user_id == u["id"])
        with self.db.connect() as c:
            return {r["subject_id"] for r in fetch_all(c, select(gov_owners.c.subject_id).where(t.subject_type == subject_type, or_(*conds)).limit(limit))}

    def with_descendants(self, ids: set[str], *, depth: int = MAX_CHAIN, limit: int = 20000, blocked: set[str] | None = None) -> set[str]:
        """``ids`` plus every asset below them in the parent tree (breadth first, bounded); assets in ``blocked`` are neither added nor expanded."""
        from olcf.intelligence.schema import assets

        out = set(ids)
        frontier = list(ids)
        for _ in range(depth):
            if not frontier or len(out) >= limit:
                break
            nxt: list[str] = []
            with self.db.connect() as c:
                for part in chunks(frontier):
                    for r in c.execute(select(assets.c.id).where(assets.c.parent_id.in_(part), assets.c.status != "removed")):
                        if r[0] not in out and (blocked is None or r[0] not in blocked):
                            out.add(r[0])
                            nxt.append(r[0])
            frontier = nxt
        return out
