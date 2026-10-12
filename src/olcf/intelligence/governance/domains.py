# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Domains (a hierarchy of business areas) and data products (curated, owned, documented groups of assets).

**Domains** (``dom_``) have a name (unique, case-insensitive), a description, an optional parent and owners. An asset has at most one *direct* domain
(``PUT /governance/assets/{id}/domain``). The *effective* domain of an asset is, in this order: its direct domain; the direct domain of the nearest ancestor (a table inherits the
domain of its lakehouse, a column that of its table - reported as ``source: "inherited"``); the domain of a data product it is a member of (``source: "product"``). Filtering
by domain honours all three and includes sub-domains.

**Data products** (``dpr_``) have a name, description, domain, status (``draft`` | ``published`` | ``deprecated``), owners, member assets (tables, views, measures, reports ...), SLA fields
(``freshness_hours``, ``availability``), a *contract* (the documented columns and expectations, stored as JSON; documentation, not enforcement) and consumers **derived from lineage**:
the assets downstream of the members, with the member they read from and the confidence of the weakest edge on the path. Quality rolls up from the members (see :mod:`quality`).
"""

from __future__ import annotations

import builtins
from typing import Any

from sqlalchemy import delete, func, insert, or_, select, update

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, new_etag
from olcf.intelligence import model as M
from olcf.intelligence.governance import flow
from olcf.intelligence.governance.common import (
    PRODUCT_MEMBER_TYPES,
    Base,
    chunks,
    clean_name,
    clean_text,
    compact_asset,
    like_escape,
    name_key,
)
from olcf.intelligence.governance.glossary import raise_precondition
from olcf.intelligence.governance.schema import (
    gov_asset_domains,
    gov_certifications,
    gov_domains,
    gov_owners,
    gov_product_members,
    gov_products,
    gov_term_links,
    gov_terms,
)
from olcf.security.rbac import Principal

PRODUCT_STATUSES = ("draft", "published", "deprecated")
MAX_MEMBERS = 2000
MAX_CONTRACT_COLUMNS = 500
MAX_EXPECTATIONS = 100


class DomainManager(Base):
    """Domains and the domain of assets."""

    def row_or_none(self, domain_id: str) -> dict[str, Any] | None:
        """The row of a domain, or ``None``."""
        with self.db.connect() as c:
            return fetch_one(c, select(gov_domains).where(gov_domains.c.id == domain_id))

    def row(self, domain_id: str) -> dict[str, Any]:
        """The row of a domain (``NotFound`` if absent)."""
        r = self.row_or_none(domain_id)
        if r is None:
            raise NotFound(f"domain '{domain_id}' does not exist")
        return r

    def resolve(self, ref: str) -> dict[str, Any] | None:
        """A domain by id or (case-insensitive) name."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_domains).where(gov_domains.c.id == ref))
            return r or fetch_one(c, select(gov_domains).where(gov_domains.c.name_key == name_key(ref)))

    def refs(self, ids: builtins.list[str]) -> dict[str, dict[str, Any]]:
        """``{id: DomainRef}``."""
        out: dict[str, dict[str, Any]] = {}
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(i for i in ids if i))):
                for r in fetch_all(c, select(gov_domains.c.id, gov_domains.c.name).where(gov_domains.c.id.in_(part))):
                    out[r["id"]] = {"id": r["id"], "name": r["name"]}
        return out

    def descendant_ids(self, domain_id: str) -> set[str]:
        """The domain and every sub-domain below it."""
        out = {domain_id}
        frontier = [domain_id]
        with self.db.connect() as c:
            while frontier:
                nxt = [r[0] for r in c.execute(select(gov_domains.c.id).where(gov_domains.c.parent_id.in_(frontier)))]
                frontier = [i for i in nxt if i not in out]
                out.update(frontier)
        return out

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def _decorate(self, rows: builtins.list[dict[str, Any]]) -> builtins.list[dict[str, Any]]:
        ids = [r["id"] for r in rows]
        owners = self.gov.owners.direct("domain", ids)
        assets_n: dict[str, int] = {}
        prod_n: dict[str, int] = {}
        kids_n: dict[str, int] = {}
        with self.db.connect() as c:
            for part in chunks(ids):
                for d, n in c.execute(select(gov_asset_domains.c.domain_id, func.count()).where(gov_asset_domains.c.domain_id.in_(part)).group_by(gov_asset_domains.c.domain_id)):
                    assets_n[d] = int(n)
                for d, n in c.execute(select(gov_products.c.domain_id, func.count()).where(gov_products.c.domain_id.in_(part)).group_by(gov_products.c.domain_id)):
                    prod_n[d] = int(n)
                for d, n in c.execute(select(gov_domains.c.parent_id, func.count()).where(gov_domains.c.parent_id.in_(part)).group_by(gov_domains.c.parent_id)):
                    kids_n[d] = int(n)
        return [{"id": r["id"], "name": r["name"], "description": r["description"], "parent_id": r["parent_id"], "owners": [self.gov.owners.public(o) for o in owners[r["id"]]],
                 "asset_count": assets_n.get(r["id"], 0), "product_count": prod_n.get(r["id"], 0), "child_count": kids_n.get(r["id"], 0), "etag": r["etag"],
                 "created_by": r["created_by"], "created_at": r["created_at"], "updated_at": r["updated_at"]} for r in rows]

    def list(self, principal: Principal, *, q: str | None = None, parent_id: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Domains by id with a keyset cursor."""
        self.org_reader(principal)
        t = gov_domains.c
        stmt = select(gov_domains)
        if q:
            stmt = stmt.where(or_(t.name_key.like(like_escape(q.lower()), escape="\\"), func.lower(t.description).like(like_escape(q.lower()), escape="\\")))
        if parent_id:
            stmt = stmt.where(t.parent_id.is_(None) if parent_id == "root" else t.parent_id == parent_id)
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id > cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return self._decorate(rows[:limit]), nxt

    def get(self, principal: Principal, domain_id: str) -> dict[str, Any]:
        """One domain with ancestors and children."""
        self.org_reader(principal)
        r = self.row(domain_id)
        out = self._decorate([r])[0]
        anc: builtins.list[dict[str, Any]] = []
        cur, seen = r["parent_id"], {r["id"]}
        while cur and cur not in seen and len(anc) < 20:
            seen.add(cur)
            p = self.row_or_none(cur)
            if p is None:
                break
            anc.append({"id": p["id"], "name": p["name"]})
            cur = p["parent_id"]
        with self.db.connect() as c:
            kids = fetch_all(c, select(gov_domains.c.id, gov_domains.c.name).where(gov_domains.c.parent_id == r["id"]).order_by(gov_domains.c.name_key))
        anc.reverse()                      # root first, like a breadcrumb
        out.update(ancestors=anc, children=[{"id": k["id"], "name": k["name"]} for k in kids])
        return out

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def _check_parent(self, parent_id: str | None, self_id: str | None) -> None:
        if parent_id is None:
            return
        if self.row_or_none(parent_id) is None:
            raise BadRequest(f"parent domain '{parent_id}' does not exist")
        seen: set[str] = set()
        cur: str | None = parent_id
        while cur and cur not in seen:
            if self_id and cur == self_id:
                raise BadRequest("the parent chain would contain the domain itself (cycle)")
            seen.add(cur)
            p = self.row_or_none(cur)
            cur = p["parent_id"] if p else None

    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Create a domain (org-wide ``governance.curate``)."""
        self.org_curator(principal)
        extra = set(body) - {"name", "description", "parent_id", "owners"}
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}", details={"allowed": ["name", "description", "parent_id", "owners"]})
        name = clean_name(body.get("name"), "name")
        self._check_parent(body.get("parent_id"), None)
        owners = [self.gov.owners.parse(o) for o in (body.get("owners") or [])]
        did, now = new_id("dom"), now_iso()
        with self.db.begin() as c:
            if fetch_one(c, select(gov_domains.c.id).where(gov_domains.c.name_key == name_key(name))):
                raise Conflict(f"a domain named '{name}' already exists")
            c.execute(insert(gov_domains).values(id=did, name=name, name_key=name_key(name), description=clean_text(body.get("description"), "description", max_len=8000) or "",
                                                 parent_id=body.get("parent_id"), etag=new_etag(), created_by=principal.id, created_at=now, updated_at=now))
            for o in owners:
                self.gov.owners._insert(c, "domain", did, o, principal.id)  # noqa: SLF001
        self.audit(principal, "governance.domain_created", resource_type="domain", resource_id=did, details={"name": name})
        return self.get(principal, did)

    def update(self, principal: Principal, domain_id: str, body: dict[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a domain (only the fields present)."""
        self.org_curator(principal)
        row = self.row(domain_id)
        self.check_etag(if_match, row["etag"])
        extra = set(body) - {"name", "description", "parent_id"}
        if extra:
            raise BadRequest(f"unknown or immutable field(s) {sorted(extra)}", details={"allowed": ["name", "description", "parent_id"]})
        vals: dict[str, Any] = {}
        if "name" in body:
            vals["name"] = clean_name(body["name"], "name")
            vals["name_key"] = name_key(vals["name"])
        if "description" in body:
            vals["description"] = clean_text(body["description"], "description", max_len=8000, allow_none=False)
        if "parent_id" in body:
            self._check_parent(body["parent_id"], row["id"])
            vals["parent_id"] = body["parent_id"]
        if not vals:
            raise BadRequest("nothing to update", remediation="send name, description and/or parent_id")
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.db.begin() as c:
            if "name_key" in vals and fetch_one(c, select(gov_domains.c.id).where(gov_domains.c.name_key == vals["name_key"], gov_domains.c.id != row["id"])):
                raise Conflict(f"a domain named '{vals['name']}' already exists")
            n = c.execute(update(gov_domains).where(gov_domains.c.id == row["id"], gov_domains.c.etag == row["etag"]).values(**vals)).rowcount
        if n == 0:
            raise_precondition(self.row(domain_id)["etag"])
        self.audit(principal, "governance.domain_updated", resource_type="domain", resource_id=row["id"], details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "name_key"))})
        return self.get(principal, domain_id)

    def delete(self, principal: Principal, domain_id: str, if_match: str | None = None) -> dict[str, Any]:
        """Delete a domain that nothing refers to."""
        self.org_curator(principal)
        row = self.row(domain_id)
        self.check_etag(if_match, row["etag"])
        with self.db.begin() as c:
            used = {
                "sub-domains": c.execute(select(func.count()).select_from(gov_domains).where(gov_domains.c.parent_id == row["id"])).scalar(),
                "assets": c.execute(select(func.count()).select_from(gov_asset_domains).where(gov_asset_domains.c.domain_id == row["id"])).scalar(),
                "data products": c.execute(select(func.count()).select_from(gov_products).where(gov_products.c.domain_id == row["id"])).scalar(),
                "glossary terms": c.execute(select(func.count()).select_from(gov_terms).where(gov_terms.c.domain_id == row["id"])).scalar(),
            }
            busy = {k: int(v or 0) for k, v in used.items() if v}
            if busy:
                raise Conflict(f"domain '{row['name']}' is still in use", details=busy, remediation="move or remove the sub-domains, assets, data products and terms first")
            c.execute(delete(gov_owners).where(gov_owners.c.subject_type == "domain", gov_owners.c.subject_id == row["id"]))
            c.execute(delete(gov_domains).where(gov_domains.c.id == row["id"]))
        self.audit(principal, "governance.domain_deleted", resource_type="domain", resource_id=row["id"], details={"name": row["name"]})
        return {"id": row["id"], "deleted": True}

    def set_asset_domain(self, principal: Principal | None, asset_id: str, domain_id: str | None, *, authorized_row: dict[str, Any] | None = None) -> dict[str, Any]:
        """Set or clear the direct domain of an asset (``governance.curate`` in its workspace)."""
        if authorized_row is None:
            assert principal is not None
            row = self.curate_asset(principal, asset_id)
        else:
            row = authorized_row
        if domain_id is not None and self.row_or_none(domain_id) is None:
            raise BadRequest(f"domain '{domain_id}' does not exist")
        t = gov_asset_domains.c
        with self.db.begin() as c:
            if domain_id is None:
                c.execute(delete(gov_asset_domains).where(t.asset_id == row["id"]))
            elif c.execute(update(gov_asset_domains).where(t.asset_id == row["id"]).values(domain_id=domain_id, assigned_by=principal.id if principal else None, assigned_at=now_iso())).rowcount == 0:
                c.execute(insert(gov_asset_domains).values(asset_id=row["id"], domain_id=domain_id, assigned_by=principal.id if principal else None, assigned_at=now_iso()))
        self.audit(principal, "governance.domain_assigned", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"], details={"domain_id": domain_id})
        return {"asset_id": row["id"], "domain_id": domain_id, "domain": self.refs([domain_id]).get(domain_id) if domain_id else None}

    # ------------------------------------------------------------------------------------------------------------------------ effective domain of assets
    def effective(self, asset_ids: builtins.list[str], principal: Principal | None = None) -> dict[str, dict[str, Any]]:
        """``{asset_id: {"domain": DomainRef, "source": "direct|inherited|product", "inherited_from": {...}|None, "product": ProductRef|None}}`` for the assets that have a domain."""
        ids = list(dict.fromkeys(asset_ids))
        if not ids:
            return {}
        chains, rows = self.gov.owners.chains(ids)
        every = sorted({a for ch in chains.values() for a in ch})
        direct: dict[str, str] = {}
        with self.db.connect() as c:
            for part in chunks(every):
                for r in c.execute(select(gov_asset_domains.c.asset_id, gov_asset_domains.c.domain_id).where(gov_asset_domains.c.asset_id.in_(part))):
                    direct[r[0]] = r[1]
        prods = self.gov.products.products_of_assets(ids)
        refs = self.refs(list(direct.values()) + [p["domain_id"] for ps in prods.values() for p in ps if p.get("domain_id")])
        out: dict[str, dict[str, Any]] = {}
        for aid in ids:
            hit: dict[str, Any] | None = None
            for dist, anc in enumerate(chains[aid]):
                if anc in direct and direct[anc] in refs:
                    src = None
                    if dist:
                        arow = rows.get(anc)
                        src = ({"asset_id": anc, "type": arow["type"], "name": arow["name"], "distance": dist}
                               if arow is not None and (principal is None or self.intel.can_see(principal, arow["workspace_id"])) else {"asset_id": None, "hidden": True, "distance": dist})
                    hit = {"domain": refs[direct[anc]], "source": "direct" if dist == 0 else "inherited", "inherited_from": src, "product": None}
                    break
            if hit is None:
                for p in prods.get(aid, []):
                    if p.get("domain_id") in refs:
                        hit = {"domain": refs[p["domain_id"]], "source": "product", "inherited_from": None, "product": {"id": p["id"], "name": p["name"], "status": p["status"]}}
                        break
            if hit is not None:
                out[aid] = hit
        return out

    def asset_ids_in(self, domain_ref: str, *, include_descendants: bool = True, limit: int = 20000) -> dict[str, str]:
        """``{asset_id: "direct"|"inherited"|"product"}`` for the assets whose *effective* domain (see :meth:`effective`) is the domain (and, by default, one of its sub-domains).

        An asset below a directly assigned asset inherits that domain unless it, or an asset between them, is assigned to another domain; a data product's domain applies to its
        members only while they have no direct or inherited domain.
        """
        d = self.resolve(domain_ref)
        if d is None:
            return {}
        dom_ids = self.descendant_ids(d["id"]) if include_descendants else {d["id"]}
        out: dict[str, str] = {}
        t = gov_asset_domains.c
        with self.db.connect() as c:
            direct = {r[0] for r in c.execute(select(t.asset_id).where(t.domain_id.in_(sorted(dom_ids))).limit(limit))}
            elsewhere = {r[0] for r in c.execute(select(t.asset_id).where(t.domain_id.not_in(sorted(dom_ids))))}
            member = {r[0] for r in c.execute(select(gov_product_members.c.asset_id).join(gov_products, gov_products.c.id == gov_product_members.c.product_id)
                                              .where(gov_products.c.domain_id.in_(sorted(dom_ids))).limit(limit))}
        for a in self.gov.owners.with_descendants(direct, limit=limit, blocked=elsewhere - direct):
            out[a] = "direct" if a in direct else "inherited"
        if member:
            eff = self.effective(sorted(member - set(out)))
            for a, e in eff.items():
                if e["source"] == "product" and e["domain"]["id"] in dom_ids:
                    out[a] = "product"
        return out

    def assets_of(self, principal: Principal, domain_id: str, *, type: str | None = None, include_descendants: bool = True, limit: int = 50,
                  cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Visible assets of a domain (``direct``, ``inherited`` from a parent asset, or through a ``product``)."""
        self.org_reader(principal)
        d = self.row(domain_id)
        found = self.asset_ids_in(d["id"], include_descendants=include_descendants)
        from olcf.intelligence.schema import assets

        ids = sorted(found)
        cur = decode_cursor(cursor)
        if cur:
            ids = [i for i in ids if i > cur[0]]
        limit = max(1, min(int(limit), 500))
        out: builtins.list[dict[str, Any]] = []
        last: str | None = None
        kinds = set(type.split(",")) if type else None
        for part in chunks(ids, 500):
            stmt = self.intel.scope_filter(select(assets).where(assets.c.id.in_(part), assets.c.status != "removed"), principal, None).order_by(assets.c.id)
            with self.db.connect() as c:
                for r in fetch_all(c, stmt):
                    if kinds and r["type"] not in kinds:
                        continue
                    out.append({**compact_asset(r), "domain_source": found[r["id"]]})
                    if len(out) > limit:
                        break
            if len(out) > limit:
                break
        nxt = None
        if len(out) > limit:
            out = out[:limit]
            last = out[-1]["id"]
            nxt = encode_cursor([last])
        return out, nxt


class ProductManager(Base):
    """Data products and their members."""

    def row_or_none(self, product_id: str) -> dict[str, Any] | None:
        """The row of a product, or ``None``."""
        with self.db.connect() as c:
            return fetch_one(c, select(gov_products).where(gov_products.c.id == product_id))

    def row(self, product_id: str) -> dict[str, Any]:
        """The row of a product (``NotFound`` if absent)."""
        r = self.row_or_none(product_id)
        if r is None:
            raise NotFound(f"data product '{product_id}' does not exist")
        return r

    def refs(self, ids: builtins.list[str]) -> dict[str, dict[str, Any]]:
        """``{id: ProductRef}``."""
        out: dict[str, dict[str, Any]] = {}
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(ids))):
                for r in fetch_all(c, select(gov_products.c.id, gov_products.c.name, gov_products.c.status).where(gov_products.c.id.in_(part))):
                    out[r["id"]] = {"id": r["id"], "name": r["name"], "status": r["status"]}
        return out

    def member_ids(self, product_id: str) -> builtins.list[str]:
        """Member asset ids of a product (sorted)."""
        with self.db.connect() as c:
            return sorted(r[0] for r in c.execute(select(gov_product_members.c.asset_id).where(gov_product_members.c.product_id == product_id)))

    def products_of_assets(self, asset_ids: builtins.list[str]) -> dict[str, builtins.list[dict[str, Any]]]:
        """Products each asset is a member of (id, name, status, domain_id)."""
        out: dict[str, builtins.list[dict[str, Any]]] = {}
        m, p = gov_product_members.c, gov_products.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(asset_ids))):
                stmt = select(m.asset_id, p.id, p.name, p.status, p.domain_id).join(gov_products, gov_products.c.id == m.product_id).where(m.asset_id.in_(part)).order_by(p.name_key)
                for r in c.execute(stmt):
                    mm = r._mapping
                    out.setdefault(mm["asset_id"], []).append({"id": mm["id"], "name": mm["name"], "status": mm["status"], "domain_id": mm["domain_id"]})
        return out

    # ------------------------------------------------------------------------------------------------------------------------ validation
    @staticmethod
    def _clean_sla(sla: Any) -> dict[str, Any]:
        if sla is None:
            return {}
        if not isinstance(sla, dict) or set(sla) - {"freshness_hours", "availability"}:
            raise BadRequest("sla must be an object with freshness_hours and/or availability")
        out: dict[str, Any] = {}
        if sla.get("freshness_hours") is not None:
            fh = sla["freshness_hours"]
            if isinstance(fh, bool) or not isinstance(fh, (int, float)) or not 0 < fh <= 87600:
                raise BadRequest("sla.freshness_hours must be a number between 0 and 87600")
            out["freshness_hours"] = fh
        if sla.get("availability") is not None:
            out["availability"] = clean_text(sla["availability"], "sla.availability", max_len=500)
        return out

    @staticmethod
    def _clean_contract(contract: Any) -> dict[str, Any]:
        if contract is None:
            return {}
        if not isinstance(contract, dict) or set(contract) - {"columns", "expectations"}:
            raise BadRequest("contract must be an object with columns and/or expectations")
        cols = contract.get("columns") or []
        exps = contract.get("expectations") or []
        if not isinstance(cols, list) or not isinstance(exps, list) or len(cols) > MAX_CONTRACT_COLUMNS or len(exps) > MAX_EXPECTATIONS:
            raise BadRequest(f"contract.columns (at most {MAX_CONTRACT_COLUMNS}) and contract.expectations (at most {MAX_EXPECTATIONS}) must be lists")
        out_cols = []
        for c in cols:
            if not isinstance(c, dict) or set(c) - {"name", "type", "description", "nullable", "classification"}:
                raise BadRequest("each contract column is {name, type?, description?, nullable?, classification?}")
            col: dict[str, Any] = {"name": clean_name(c.get("name"), "contract column name", max_len=255)}
            for k in ("type", "description", "classification"):
                if c.get(k) is not None:
                    col[k] = clean_text(c[k], f"contract column {k}", max_len=2000)
            if c.get("nullable") is not None:
                if not isinstance(c["nullable"], bool):
                    raise BadRequest("contract column nullable must be true or false")
                col["nullable"] = c["nullable"]
            out_cols.append(col)
        out_exps = []
        for e in exps:
            if not isinstance(e, dict) or set(e) - {"name", "description", "rule"}:
                raise BadRequest("each expectation is {name, description?, rule?}")
            ex: dict[str, Any] = {"name": clean_name(e.get("name"), "expectation name", max_len=255)}
            for k in ("description", "rule"):
                if e.get(k) is not None:
                    ex[k] = clean_text(e[k], f"expectation {k}", max_len=2000)
            out_exps.append(ex)
        return {"columns": out_cols, "expectations": out_exps}

    def _check_members(self, principal: Principal, asset_ids: Any) -> builtins.list[dict[str, Any]]:
        if not isinstance(asset_ids, list) or not all(isinstance(x, str) for x in asset_ids):
            raise BadRequest("asset_ids must be a list of asset ids")
        ids = list(dict.fromkeys(asset_ids))
        if len(ids) > MAX_MEMBERS:
            raise BadRequest(f"a data product can have at most {MAX_MEMBERS} members")
        rows = []
        for i in ids:
            r = self.asset(principal, i)
            if r["type"] not in PRODUCT_MEMBER_TYPES:
                raise BadRequest(f"a {r['type']} cannot be a member of a data product ('{r['name']}')", details={"supported": list(PRODUCT_MEMBER_TYPES)})
            if r["status"] == "removed":
                raise BadRequest(f"asset '{r['name']}' was removed from the graph")
            rows.append(r)
        return rows

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def _decorate(self, rows: builtins.list[dict[str, Any]]) -> builtins.list[dict[str, Any]]:
        ids = [r["id"] for r in rows]
        certs = self.gov.certification.states("product", ids)
        counts: dict[str, int] = {}
        with self.db.connect() as c:
            for part in chunks(ids):
                for pid, n in c.execute(select(gov_product_members.c.product_id, func.count()).where(gov_product_members.c.product_id.in_(part)).group_by(gov_product_members.c.product_id)):
                    counts[pid] = int(n)
        doms = self.gov.domains.refs([r["domain_id"] for r in rows if r["domain_id"]])
        owners = self.gov.owners.direct("product", ids)
        return [{"id": r["id"], "name": r["name"], "description": r["description"], "domain": doms.get(r["domain_id"]) if r["domain_id"] else None, "status": r["status"],
                 "certification": certs.get(r["id"], "none"), "sla": r["sla"] or {}, "contract": r["contract"] or {}, "member_count": counts.get(r["id"], 0),
                 "owners": [self.gov.owners.public(o) for o in owners[r["id"]]], "etag": r["etag"], "created_by": r["created_by"], "created_at": r["created_at"],
                 "updated_at": r["updated_at"]} for r in rows]

    def list(self, principal: Principal, *, q: str | None = None, status: str | None = None, domain_id: str | None = None, certification: str | None = None,
             owner: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Data products with a keyset cursor."""
        self.org_reader(principal)
        t = gov_products.c
        stmt = select(gov_products)
        if q:
            like = like_escape(q.lower())
            stmt = stmt.where(or_(t.name_key.like(like, escape="\\"), func.lower(t.description).like(like, escape="\\")))
        if status:
            if status not in PRODUCT_STATUSES:
                raise BadRequest(f"status must be one of {list(PRODUCT_STATUSES)}")
            stmt = stmt.where(t.status == status)
        if domain_id:
            stmt = stmt.where(t.domain_id.in_(sorted(self.gov.domains.descendant_ids(domain_id))))
        if certification:
            if certification not in ("none", "draft", "certified", "deprecated"):
                raise BadRequest("certification must be one of none, draft, certified, deprecated")
            c_ = gov_certifications.c
            stmt = stmt.where(t.id.not_in(select(c_.subject_id).where(c_.subject_type == "product", c_.state != "none")) if certification == "none"
                              else t.id.in_(select(c_.subject_id).where(c_.subject_type == "product", c_.state == certification)))
        if owner:
            ids = self.gov.owners.subjects_for("product", owner)
            stmt = stmt.where(t.id.in_(sorted(ids)) if ids else t.id == "")
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id > cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return self._decorate(rows[:limit]), nxt

    def members(self, principal: Principal, product_id: str) -> tuple[builtins.list[dict[str, Any]], int]:
        """``(visible member AssetRefs, number of members the caller cannot see)``."""
        ids = self.member_ids(product_id)
        vis = self.visible_rows(principal, ids)
        return [compact_asset(vis[i]) for i in ids if i in vis and vis[i]["status"] != "removed"], len([i for i in ids if i not in vis])

    def get(self, principal: Principal, product_id: str) -> dict[str, Any]:
        """One product with members, consumers (first 50), quality score, owners and terms."""
        self.org_reader(principal)
        r = self.row(product_id)
        out = self._decorate([r])[0]
        members, hidden = self.members(principal, r["id"])
        out["members"] = members
        out["hidden_members"] = hidden
        cons = self.consumers(principal, r["id"], depth=3, limit=50)
        out["consumers"] = cons["items"]
        out["consumers_truncated"] = cons["truncated"]
        out["terms"] = self.gov.glossary.terms_of_products([r["id"]])[r["id"]]
        try:
            out["quality"] = {"score": self.gov.quality.product(principal, r["id"])["score"]}
        except Exception:  # noqa: BLE001 - a scoring problem never hides the product itself
            out["quality"] = None
        return out

    def consumers(self, principal: Principal, product_id: str, *, depth: int = 3, min_confidence: str | None = None, limit: int = 200) -> dict[str, Any]:
        """Assets downstream of the product's members that are not members themselves, derived from lineage: ``{"items": [{asset, via_member, confidence, hops}], "truncated"}``.

        Only assets the caller can see are listed. ``confidence`` is the weakest edge on the strongest path from the member."""
        self.org_reader(principal)
        self.row(product_id)
        mids = [i for i in self.member_ids(product_id) if i in self.visible_rows(principal, [i])]
        if not mids:
            return {"product_id": product_id, "items": [], "truncated": False}
        walk = self.intel.graph.walk(mids, direction="down", depth=max(1, min(int(depth), 6)), min_confidence=min_confidence, keep_node=self.intel.keep_node(principal))
        found = flow.reach(walk.edges.values(), mids, forward=True)
        member_set = set(mids)
        items: builtins.list[dict[str, Any]] = []
        for nid, rc in found.items():
            row = walk.nodes.get(nid, {}).get("row")
            if nid in member_set or row is None or row["type"] in M.PROCESS_TYPES or row["status"] == "removed":
                continue
            origin = walk.nodes.get(rc.origin, {}).get("row")
            items.append({"asset": compact_asset(row), "via_member": compact_asset(origin) if origin else None, "confidence": rc.floor, "hops": rc.hops})
        items.sort(key=lambda x: (flow.RANK[x["confidence"]], x["hops"], x["asset"]["name"].lower()))
        return {"product_id": product_id, "items": items[: max(1, min(int(limit), 1000))], "total": len(items), "truncated": walk.truncated or len(items) > limit}

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Create a data product (org-wide ``governance.curate``; ``published`` needs ``governance.approve``)."""
        self.org_curator(principal)
        allowed = {"name", "description", "domain_id", "status", "owners", "member_asset_ids", "sla", "contract"}
        extra = set(body) - allowed
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}", details={"allowed": sorted(allowed)})
        name = clean_name(body.get("name"), "name")
        status = body.get("status", "draft")
        if status not in PRODUCT_STATUSES:
            raise BadRequest(f"status must be one of {list(PRODUCT_STATUSES)}")
        if status == "published":
            principal.require("governance.approve")
        if body.get("domain_id") and self.gov.domains.row_or_none(body["domain_id"]) is None:
            raise BadRequest(f"domain '{body['domain_id']}' does not exist")
        members = self._check_members(principal, body.get("member_asset_ids") or [])
        owners = [self.gov.owners.parse(o) for o in (body.get("owners") or [])]
        pid, now = new_id("dpr"), now_iso()
        with self.db.begin() as c:
            if fetch_one(c, select(gov_products.c.id).where(gov_products.c.name_key == name_key(name))):
                raise Conflict(f"a data product named '{name}' already exists")
            c.execute(insert(gov_products).values(id=pid, name=name, name_key=name_key(name), description=clean_text(body.get("description"), "description", max_len=8000) or "",
                                                  domain_id=body.get("domain_id"), status=status, sla=self._clean_sla(body.get("sla")), contract=self._clean_contract(body.get("contract")),
                                                  etag=new_etag(), created_by=principal.id, created_at=now, updated_at=now))
            for m in members:
                c.execute(insert(gov_product_members).values(product_id=pid, asset_id=m["id"], added_by=principal.id, added_at=now))
            for o in owners:
                self.gov.owners._insert(c, "product", pid, o, principal.id)  # noqa: SLF001
        self.audit(principal, "governance.product_created", resource_type="product", resource_id=pid, details={"name": name, "status": status, "members": len(members)})
        return self.get(principal, pid)

    def update(self, principal: Principal, product_id: str, body: dict[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a product (only the fields present)."""
        self.org_curator(principal)
        row = self.row(product_id)
        self.check_etag(if_match, row["etag"])
        allowed = {"name", "description", "domain_id", "status", "sla", "contract"}
        extra = set(body) - allowed
        if extra:
            raise BadRequest(f"unknown or immutable field(s) {sorted(extra)}", details={"allowed": sorted(allowed), "hint": "members and owners have their own routes"})
        vals: dict[str, Any] = {}
        if "name" in body:
            vals["name"] = clean_name(body["name"], "name")
            vals["name_key"] = name_key(vals["name"])
        if "description" in body:
            vals["description"] = clean_text(body["description"], "description", max_len=8000, allow_none=False)
        if "domain_id" in body:
            if body["domain_id"] and self.gov.domains.row_or_none(body["domain_id"]) is None:
                raise BadRequest(f"domain '{body['domain_id']}' does not exist")
            vals["domain_id"] = body["domain_id"]
        if "status" in body:
            if body["status"] not in PRODUCT_STATUSES:
                raise BadRequest(f"status must be one of {list(PRODUCT_STATUSES)}")
            if body["status"] == "published" and row["status"] != "published":
                principal.require("governance.approve")
            vals["status"] = body["status"]
        if "sla" in body:
            vals["sla"] = self._clean_sla(body["sla"])
        if "contract" in body:
            vals["contract"] = self._clean_contract(body["contract"])
        if not vals:
            raise BadRequest("nothing to update", remediation="send at least one of " + ", ".join(sorted(allowed)))
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.db.begin() as c:
            if "name_key" in vals and fetch_one(c, select(gov_products.c.id).where(gov_products.c.name_key == vals["name_key"], gov_products.c.id != row["id"])):
                raise Conflict(f"a data product named '{vals['name']}' already exists")
            n = c.execute(update(gov_products).where(gov_products.c.id == row["id"], gov_products.c.etag == row["etag"]).values(**vals)).rowcount
        if n == 0:
            raise_precondition(self.row(product_id)["etag"])
        self.audit(principal, "governance.product_updated", resource_type="product", resource_id=row["id"], details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "name_key"))})
        return self.get(principal, product_id)

    def delete(self, principal: Principal, product_id: str, if_match: str | None = None) -> dict[str, Any]:
        """Delete a product that is not published."""
        self.org_curator(principal)
        row = self.row(product_id)
        self.check_etag(if_match, row["etag"])
        if row["status"] == "published":
            raise Conflict(f"data product '{row['name']}' is published", remediation="set its status to deprecated first")
        with self.db.begin() as c:
            c.execute(delete(gov_product_members).where(gov_product_members.c.product_id == row["id"]))
            c.execute(delete(gov_owners).where(gov_owners.c.subject_type == "product", gov_owners.c.subject_id == row["id"]))
            c.execute(delete(gov_certifications).where(gov_certifications.c.subject_type == "product", gov_certifications.c.subject_id == row["id"]))
            c.execute(delete(gov_term_links).where(gov_term_links.c.subject_type == "product", gov_term_links.c.subject_id == row["id"]))
            c.execute(delete(gov_products).where(gov_products.c.id == row["id"]))
        self.audit(principal, "governance.product_deleted", resource_type="product", resource_id=row["id"], details={"name": row["name"]})
        return {"id": row["id"], "deleted": True}

    def set_members(self, principal: Principal, product_id: str, asset_ids: Any) -> dict[str, Any]:
        """Replace the member set."""
        self.org_curator(principal)
        row = self.row(product_id)
        members = self._check_members(principal, asset_ids)
        wanted = {m["id"] for m in members}
        now = now_iso()
        t = gov_product_members.c
        with self.db.begin() as c:
            have = {r[0] for r in c.execute(select(t.asset_id).where(t.product_id == row["id"]))}
            # members the caller cannot see are not touched: replacing the visible set must not silently drop what it cannot know about
            hidden = {a for a in have if a not in self.visible_rows(principal, [a])}
            c.execute(delete(gov_product_members).where(t.product_id == row["id"], t.asset_id.in_(sorted(have - wanted - hidden)) if have - wanted - hidden else t.asset_id == ""))
            for a in sorted(wanted - have):
                c.execute(insert(gov_product_members).values(product_id=row["id"], asset_id=a, added_by=principal.id, added_at=now))
            c.execute(update(gov_products).where(gov_products.c.id == row["id"]).values(updated_at=now, etag=new_etag()))
        self.audit(principal, "governance.product_members_changed", resource_type="product", resource_id=row["id"], details={"added": len(wanted - have), "removed": len(have - wanted - hidden)})
        return self.get(principal, product_id)

    def add_member(self, principal: Principal, product_id: str, asset_id: str) -> dict[str, Any]:
        """Add one member."""
        self.org_curator(principal)
        row = self.row(product_id)
        (m,) = self._check_members(principal, [asset_id])
        with self.db.begin() as c:
            if fetch_one(c, select(gov_product_members.c.asset_id).where(gov_product_members.c.product_id == row["id"], gov_product_members.c.asset_id == m["id"])):
                raise Conflict(f"'{m['name']}' is already a member of '{row['name']}'")
            if int(c.execute(select(func.count()).select_from(gov_product_members).where(gov_product_members.c.product_id == row["id"])).scalar() or 0) >= MAX_MEMBERS:
                raise BadRequest(f"a data product can have at most {MAX_MEMBERS} members")
            c.execute(insert(gov_product_members).values(product_id=row["id"], asset_id=m["id"], added_by=principal.id, added_at=now_iso()))
            c.execute(update(gov_products).where(gov_products.c.id == row["id"]).values(updated_at=now_iso(), etag=new_etag()))
        self.audit(principal, "governance.product_members_changed", resource_type="product", resource_id=row["id"], workspace_id=m["workspace_id"], details={"added": [m["id"]]})
        return self.get(principal, product_id)

    def remove_member(self, principal: Principal, product_id: str, asset_id: str) -> dict[str, Any]:
        """Remove one member."""
        self.org_curator(principal)
        row = self.row(product_id)
        t = gov_product_members.c
        with self.db.begin() as c:
            n = c.execute(delete(gov_product_members).where(t.product_id == row["id"], t.asset_id == asset_id)).rowcount
            if n == 0:
                raise NotFound(f"asset '{asset_id}' is not a member of '{row['name']}'")
            c.execute(update(gov_products).where(gov_products.c.id == row["id"]).values(updated_at=now_iso(), etag=new_etag()))
        self.audit(principal, "governance.product_members_changed", resource_type="product", resource_id=row["id"], details={"removed": [asset_id]})
        return self.get(principal, product_id)
