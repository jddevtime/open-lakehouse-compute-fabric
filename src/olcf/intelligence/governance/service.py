# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``GovernanceService``: the facade the Intelligence core and the REST routes use (``intelligence_service.governance``).

It owns the managers (``owners``, ``glossary``, ``domains``, ``products``, ``classifications``, ``propagation``, ``certification``, ``workflow``, ``policies``, ``quality``) and implements

* the **hooks** the Intelligence core calls when present: :meth:`decorate` (``governance_info``), :meth:`owners_of` and :meth:`asset_filter`;
* the **views** that combine several managers: :meth:`summary`, :meth:`asset_detail` and :meth:`asset_graph` (virtual ``GOVERNED_BY`` / ``CLASSIFIED_AS`` edges that are served from
  governance tables and never stored in the lineage graph);
* the **background work** of :meth:`tick`: the event bus subscriber :meth:`on_event` only appends a row to ``gov_signals`` (it runs inside the publisher's call and must be cheap and
  never raise); a leader-only loop turns signals into work - suggestions after a scan, the certification sweep after a schema change, policy evaluation, retention - and does a periodic
  certification sweep. Nothing here is called from the compute, SQL, pipeline or lineage-ingest path.

If the governance tables are missing (migration not applied or dropped) every hook answers "nothing" (decorate / owners) or ``Unavailable`` (asset_filter), the loop idles, and the health
check reports it; the Intelligence module itself keeps working.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, insert, inspect, select, update
from sqlalchemy.exc import SQLAlchemyError

from olcf.core.errors import BadRequest, Unavailable
from olcf.core.health import ComponentHealth, Status
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all
from olcf.intelligence.governance.classification import ClassificationManager
from olcf.intelligence.governance.common import CERT_STATES, DATASET_ASSET_TYPES, Base, chunks, compact_asset
from olcf.intelligence.governance.domains import DomainManager, ProductManager
from olcf.intelligence.governance.glossary import GlossaryManager
from olcf.intelligence.governance.ownership import OwnershipManager
from olcf.intelligence.governance.policies import PolicyManager
from olcf.intelligence.governance.propagation import Propagation
from olcf.intelligence.governance.quality import QualityManager
from olcf.intelligence.governance.schema import (
    GOVERNANCE_TABLES,
    gov_certifications,
    gov_class_assignments,
    gov_domains,
    gov_products,
    gov_signals,
    gov_term_links,
    gov_terms,
)
from olcf.intelligence.governance.workflow import CertificationManager, WorkflowManager
from olcf.intelligence.schema import assets
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.intelligence.governance")

TABLE_CHECK_SECONDS = 20.0
SWEEP_SECONDS = 300.0
SCHEMA_WINDOW_SECONDS = 120.0
SIGNAL_BATCH = 50
MAX_SIGNAL_ATTEMPTS = 3
SUMMARY_CAP = 20000


class GovernanceService:
    """See the module docstring."""

    def __init__(self, ctx: Any, intel: Any) -> None:
        self.ctx, self.db, self.intel = ctx, ctx.db, intel
        self.owners = OwnershipManager(self)
        self.glossary = GlossaryManager(self)
        self.domains = DomainManager(self)
        self.products = ProductManager(self)
        self.propagation = Propagation(self)
        self.classifications = ClassificationManager(self)
        self.certification = CertificationManager(self)
        self.workflow = WorkflowManager(self)
        self.policies = PolicyManager(self)
        self.quality = QualityManager(self)
        self._base = Base(self)
        self._lock = threading.Lock()
        self._tables_checked = 0.0
        self._tables_ok = True
        self._missing: list[str] = []
        self._last_sweep = 0.0
        self._schema_until = 0.0
        self._last_purge = 0.0
        self.last_tick_at: str | None = None
        self.last_error: str | None = None
        self.stats: dict[str, int] = {"signals_done": 0, "signals_failed": 0, "ticks": 0, "invalidated": 0}

    # ------------------------------------------------------------------------------------------------------------------------ availability
    def tables_available(self, *, force: bool = False) -> bool:
        """True if every governance table exists (checked at most every ``TABLE_CHECK_SECONDS``)."""
        now = time.monotonic()
        if not force and now - self._tables_checked < TABLE_CHECK_SECONDS:
            return self._tables_ok
        try:
            insp = inspect(self.db.engine)
            missing = [t.name for t in GOVERNANCE_TABLES if not insp.has_table(t.name)]
        except Exception:  # noqa: BLE001 - if we cannot even ask, say so
            log.exception("could not check the governance tables")
            missing = ["(database unreachable)"]
        self._missing, self._tables_ok, self._tables_checked = missing, not missing, now
        return self._tables_ok

    def require_tables(self) -> None:
        """``Unavailable`` (503) with a remediation when the governance tables are missing."""
        if not self.tables_available():
            raise Unavailable("governance storage is not available: " + ", ".join(self._missing[:5]), remediation="run 'olcf db migrate' (migration stream 'governance') and restart")

    def guard(self, exc: BaseException) -> Unavailable:
        """Translate a database error into ``Unavailable`` and re-check the tables."""
        self.tables_available(force=True)
        return Unavailable("governance storage is not available" + (": " + ", ".join(self._missing[:5]) if self._missing else ""),
                           remediation="run 'olcf db migrate' (migration stream 'governance'); if the database itself is down, check 'olcf health'")

    # ------------------------------------------------------------------------------------------------------------------------ hooks for the Intelligence core
    def decorate(self, principal: Principal, asset_ids: list[str]) -> dict[str, dict[str, Any]]:
        """``governance_info``: per asset ``{"domain", "certification", "classifications", "owners", "glossary_terms", "data_product", "violations"}``.

        Bulk: a fixed number of queries per call. Classifications are the direct approved labels plus the labels that provably reach the asset by lineage (``source: "inherited"``).
        Returns ``{}`` when the governance tables are missing.
        """
        ids = list(dict.fromkeys(asset_ids))
        if not ids or not self.tables_available():
            return {}
        rows = self._base.visible_rows(principal, ids)
        ids = [i for i in ids if i in rows]
        if not ids:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for part in chunks(ids, 200):
            dom = self.domains.effective(part, principal)
            cert = self.certification.states("asset", part)
            own = self.owners.effective(part, principal)
            terms = self.glossary.terms_of_assets(part)
            prods = self.products.products_of_assets(part)
            direct = self.classifications.rows_for_assets(part)
            inherited = self.propagation.inherited_for(part, proven_only=True)
            refs = self.classifications.refs({r["class_id"] for rs in direct.values() for r in rs} | {i.class_id for its in inherited.values() for i in its})
            viol = self.policies.counts_for(part)
            for aid in part:
                cls: list[dict[str, Any]] = []
                seen: set[str] = set()
                for r in direct.get(aid, []):
                    if r["class_id"] in refs:
                        cls.append({**refs[r["class_id"]], "source": r["source"], "inherited": False, "confidence": r["confidence"]})
                        seen.add(r["class_id"])
                for it in inherited.get(aid, []):
                    if it.class_id in refs and it.class_id not in seen:
                        cls.append({**refs[it.class_id], "source": "inherited", "inherited": True, "confidence": it.confidence, "precision": it.precision})
                        seen.add(it.class_id)
                cls.sort(key=lambda x: (-x["level"], x["name"]))
                d = dom.get(aid)
                out[aid] = {
                    "domain": {**d["domain"], "source": d["source"]} if d else None,
                    "certification": cert.get(aid, "none"),
                    "classifications": cls,
                    "owners": own.get(aid, []),
                    "glossary_terms": [{"id": t["id"], "name": t["name"], "status": t["status"]} for t in terms.get(aid, [])],
                    "data_product": [{"id": p["id"], "name": p["name"], "status": p["status"]} for p in prods.get(aid, [])],
                    "violations": viol.get(aid, {"open": 0, "by_severity": {}}),
                }
        return out

    def decorate_compact(self, principal: Principal, asset_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Compact governance facts for catalog list cards, one bulk call per page (``GET /intelligence/assets`` embeds the result as ``governance``).

        Per asset: ``classifications`` (``[{"name", "level", "inherited"}]``, highest level first), ``certification``, ``domain`` (``{"id", "name"}`` or ``None``), ``quality_score`` (0..100 or
        ``None``; the same figure ``GET /governance/assets/{id}/quality`` reports) and ``owners`` (number of effective owners). Built from :meth:`decorate` plus one bulk quality computation; if
        the quality computation fails the other facts are still returned with ``quality_score: None``.
        """
        full = self.decorate(principal, asset_ids)
        if not full:
            return {}
        try:
            scores = self.quality.scores(list(full))
        except Exception:  # noqa: BLE001 - a quality failure never hides the other facts
            log.exception("quality scores for the asset list failed")
            scores = {}
        out: dict[str, dict[str, Any]] = {}
        for aid, g in full.items():
            q = scores.get(aid)
            dom = g.get("domain")
            out[aid] = {
                "classifications": [{"name": c["name"], "level": c["level"], "inherited": bool(c.get("inherited"))} for c in g["classifications"]],
                "certification": g["certification"],
                "domain": {"id": dom["id"], "name": dom["name"]} if dom else None,
                "quality_score": q["score"] if q else None,
                "owners": len(g["owners"]),
            }
        return out

    def owners_of(self, principal: Principal, asset_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Effective owners per asset (only assets that have some); ``{}`` when the governance tables are missing."""
        ids = list(dict.fromkeys(asset_ids))
        if not ids or not self.tables_available():
            return {}
        rows = self._base.visible_rows(principal, ids)
        eff = self.owners.effective([i for i in ids if i in rows], principal)
        return {k: v for k, v in eff.items() if v}

    def asset_filter(self, principal: Principal, *, domain: str | None = None, classification: str | None = None, certification: str | None = None, owner: str | None = None) -> set[str]:
        """Ids of the assets that match **every** given filter (the Intelligence core then applies visibility and its own filters).

        ``domain``: direct, inherited from a parent asset, or through a data product, sub-domains included. ``classification``: approved direct labels, labels that provably reach the
        asset by lineage, and the containers (table, model, report) of a directly labelled column. ``certification``: ``none|draft|certified|deprecated`` (``none`` = every visible asset
        without a certification row). ``owner``: a user id, user name, e-mail or external name (``me`` = the caller), by effective ownership.
        """
        self.require_tables()
        if certification is not None and certification not in CERT_STATES:
            raise BadRequest(f"certification must be one of {list(CERT_STATES)}")
        try:
            sets: list[set[str]] = []
            if domain:
                sets.append(set(self.domains.asset_ids_in(domain, limit=SUMMARY_CAP * 5)))
            if classification:
                sets.append(self._assets_with_class(classification))
            if certification:
                sets.append(self._assets_with_state(principal, certification))
            if owner:
                sets.append(self.owners.asset_ids_for(principal.id if owner == "me" else owner, limit=SUMMARY_CAP * 5))
        except SQLAlchemyError as exc:
            raise self.guard(exc) from exc
        if not sets:
            return set()
        out = sets[0]
        for s in sets[1:]:
            out = out & s
        return out

    def _assets_with_class(self, ref: str) -> set[str]:
        cls = self.classifications.row_or_none(ref)
        if cls is None:
            return set()
        g = gov_class_assignments.c
        with self.db.connect() as c:
            direct = {r[0] for r in c.execute(select(g.asset_id).where(g.class_id == cls["id"], g.status == "approved"))}
            parents: set[str] = set()
            if direct:
                for part in chunks(sorted(direct)):
                    parents.update(r[0] for r in c.execute(select(assets.c.parent_id).where(assets.c.id.in_(part), assets.c.type == "column", assets.c.parent_id.is_not(None))))
        inherited = {i.target_id for i in self.propagation.result().by_class.get(cls["id"], []) if i.proven}
        return direct | parents | inherited

    def _assets_with_state(self, principal: Principal, state: str) -> set[str]:
        t = gov_certifications.c
        with self.db.connect() as c:
            if state != "none":
                return {r[0] for r in c.execute(select(t.subject_id).where(t.subject_type == "asset", t.state == state))}
            held = {r[0] for r in c.execute(select(t.subject_id).where(t.subject_type == "asset", t.state != "none"))}
            stmt = self.intel.scope_filter(select(assets.c.id).where(assets.c.status != "removed"), principal, None).limit(SUMMARY_CAP * 5)
            return {r[0] for r in c.execute(stmt)} - held

    # ------------------------------------------------------------------------------------------------------------------------ per-asset view
    def asset_detail(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """``GET /governance/assets/{id}``: everything governance knows about one visible asset."""
        row = self._base.asset(principal, asset_id)
        aid = row["id"]
        dom = self.domains.effective([aid], principal).get(aid)
        prods = self.products.products_of_assets([aid]).get(aid, [])
        pdom = self.domains.refs([p["domain_id"] for p in prods if p.get("domain_id")])
        domains = []
        if dom:
            domains.append({"domain": dom["domain"], "source": dom["source"], "inherited_from": dom["inherited_from"], "product": dom.get("product")})
        for p in prods:
            if p.get("domain_id") in pdom and not any(d["domain"]["id"] == p["domain_id"] for d in domains):
                domains.append({"domain": pdom[p["domain_id"]], "source": "product", "inherited_from": None, "product": {"id": p["id"], "name": p["name"], "status": p["status"]}})
        pending = [t for t in self.glossary.terms_of_assets([aid], include_pending=True).get(aid, []) if t["link_status"] == "pending"]
        score = self.quality.for_asset(principal, aid)
        return {"asset": compact_asset(row), "domain": ({"domain": dom["domain"], "source": dom["source"], "inherited_from": dom["inherited_from"]} if dom else None), "domains": domains,
                "products": [{"id": p["id"], "name": p["name"], "status": p["status"]} for p in prods],
                "terms": self.glossary.terms_of_assets([aid]).get(aid, []), "pending_term_links": pending,
                "owners": {"direct": [self.owners.public(r) for r in self.owners.direct("asset", [aid])[aid]], "effective": self.owners.effective([aid], principal)[aid]},
                "classifications": self.classifications.for_asset(principal, aid), "certification": self.certification.get(principal, "asset", aid),
                "quality": {"score": score["score"], "own_score": score.get("own_score")}, "violations": self.policies.list_for_asset(principal, aid),
                "open_changes": self.workflow.for_subject(principal, "asset", aid)}

    def asset_graph(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """``GET /governance/assets/{id}/graph``: virtual ``GOVERNED_BY`` and ``CLASSIFIED_AS`` edges of one asset. They are computed from governance tables, never stored."""
        row = self._base.asset(principal, asset_id)
        aid = row["id"]
        nodes: dict[str, dict[str, Any]] = {aid: {"id": aid, "type": "asset", "name": row["name"]}}
        edges_out: list[dict[str, Any]] = []
        dom = self.domains.effective([aid], principal).get(aid)
        if dom:
            d = dom["domain"]
            nodes[d["id"]] = {"id": d["id"], "type": "domain", "name": d["name"]}
            edges_out.append({"from": aid, "to": d["id"], "type": "GOVERNED_BY", "virtual": True, "attrs": {"via": dom["source"], "inherited_from": dom["inherited_from"]}})
        for p in self.products.products_of_assets([aid]).get(aid, []):
            nodes[p["id"]] = {"id": p["id"], "type": "data_product", "name": p["name"]}
            edges_out.append({"from": aid, "to": p["id"], "type": "GOVERNED_BY", "virtual": True, "attrs": {"via": "member", "status": p["status"]}})
        for t in self.glossary.terms_of_assets([aid]).get(aid, []):
            nodes[t["id"]] = {"id": t["id"], "type": "glossary_term", "name": t["name"]}
            edges_out.append({"from": aid, "to": t["id"], "type": "GOVERNED_BY", "virtual": True, "attrs": {"via": "term_link", "source": t["source"], "confidence": t["confidence"]}})
        direct = self.classifications.rows_for_assets([aid])[aid]
        inherited = self.propagation.inherited_for([aid], proven_only=True).get(aid, [])
        refs = self.classifications.refs({r["class_id"] for r in direct} | {i.class_id for i in inherited})
        for r in direct:
            if r["class_id"] in refs:
                c = refs[r["class_id"]]
                nodes[c["id"]] = {"id": c["id"], "type": "classification", "name": c["name"]}
                edges_out.append({"from": aid, "to": c["id"], "type": "CLASSIFIED_AS", "virtual": True, "attrs": {"source": r["source"], "status": r["status"], "precision": "asset", "confidence": r["confidence"]}})
        have = {r["class_id"] for r in direct}
        for it in inherited:
            if it.class_id in refs and it.class_id not in have:
                c = refs[it.class_id]
                nodes[c["id"]] = {"id": c["id"], "type": "classification", "name": c["name"]}
                src = self.intel.graph.asset_rows([it.source_id]).get(it.source_id)
                edges_out.append({"from": aid, "to": c["id"], "type": "CLASSIFIED_AS", "virtual": True,
                                  "attrs": {"source": "inherited", "status": "approved", "precision": it.precision, "confidence": it.confidence, "confidence_floor": it.floor,
                                            "from_asset": self._base.ref(principal, src)}})
        return {"asset_id": aid, "nodes": list(nodes.values()), "edges": edges_out}

    # ------------------------------------------------------------------------------------------------------------------------ overview
    def summary(self, principal: Principal, workspace_id: str | None = None) -> dict[str, Any]:
        """``GET /governance/summary``: counts and coverage over what the caller can see."""
        self.require_tables()
        self.intel.scope(principal)
        t, g = gov_terms.c, gov_class_assignments.c
        with self.db.connect() as c:
            terms = {s: int(n) for s, n in c.execute(select(t.status, func.count()).group_by(t.status))}
            n_domains = int(c.execute(select(func.count()).select_from(gov_domains)).scalar() or 0)
            prods = {s: int(n) for s, n in c.execute(select(gov_products.c.status, func.count()).group_by(gov_products.c.status))}
            visible = self._base.scope_ids(principal, workspace_id)
            assign = {s: int(n) for s, n in c.execute(select(g.status, func.count()).where(g.asset_id.in_(visible)).group_by(g.status))}
            by_label = {name: int(n) for name, n in c.execute(select(self.classifications_table.c.name, func.count()).join(gov_class_assignments, g.class_id == self.classifications_table.c.id)
                                                              .where(g.status == "approved", g.asset_id.in_(visible)).group_by(self.classifications_table.c.name))}
            labels = int(c.execute(select(func.count()).select_from(self.classifications_table)).scalar() or 0)
            cert = {s: int(n) for s, n in c.execute(select(gov_certifications.c.state, func.count()).group_by(gov_certifications.c.state))}
            last_eval = c.execute(select(func.max(self.policies_table.c.last_evaluated_at))).scalar()
        coverage = self._coverage(principal, workspace_id)
        return {"glossary": {"terms": sum(terms.values()), "by_status": terms}, "domains": n_domains, "products": {"total": sum(prods.values()), "by_status": prods},
                "classifications": {"labels": labels, "assignments": {"approved": assign.get("approved", 0), "suggested": assign.get("suggested", 0), "rejected": assign.get("rejected", 0)},
                                    "by_label": dict(sorted(by_label.items()))},
                "review_queue": self.classifications.queue_counts(principal, workspace_id), "violations": self.policies.summary(principal, workspace_id),
                "certification": {"by_state": {s: cert.get(s, 0) for s in ("draft", "certified", "deprecated")}}, "coverage": coverage["coverage"],
                "top_unowned": coverage["top_unowned"], "top_unclassified": coverage["top_unclassified"], "evaluation": {"last_evaluated_at": last_eval}, "health": self.health_labels()}

    @property
    def classifications_table(self) -> Any:
        """The ``gov_classifications`` table (kept as an attribute so the summary reads naturally)."""
        from olcf.intelligence.governance.schema import gov_classifications

        return gov_classifications

    @property
    def policies_table(self) -> Any:
        """The ``gov_policies`` table."""
        from olcf.intelligence.governance.schema import gov_policies

        return gov_policies

    def _coverage(self, principal: Principal, workspace_id: str | None) -> dict[str, Any]:
        a = assets.c
        stmt = self.intel.scope_filter(select(a.id, a.name, a.type, a.path, a.workspace_id, a.description).where(a.status == "active", a.type.in_(DATASET_ASSET_TYPES)), principal, workspace_id)
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(a.id).limit(SUMMARY_CAP + 1))
        sampled = len(rows) > SUMMARY_CAP
        rows = rows[:SUMMARY_CAP]
        ids = [r["id"] for r in rows]
        n = len(ids)
        if not n:
            return {"coverage": {"datasets": 0, "owner_pct": None, "description_pct": None, "domain_pct": None, "term_pct": None, "classified_pct": None, "classification_reviewed_pct": None, "datasets_with_pending_suggestions": 0, "certified_pct": None,
                                 "sampled": False}, "top_unowned": [], "top_unclassified": []}
        owned: set[str] = set()
        with_domain: set[str] = set()
        for part in chunks(ids, 500):
            owned.update(k for k, v in self.owners.effective(part).items() if any(o["role"] == "owner" for o in v))
            with_domain.update(self.domains.effective(part).keys())
        termed: set[str] = set()
        certified: set[str] = set()
        pending: dict[str, int] = {}
        classified: set[str] = set()
        tl, cs, g = gov_term_links.c, gov_certifications.c, gov_class_assignments.c
        with self.db.connect() as c:
            for part in chunks(ids):
                termed.update(r[0] for r in c.execute(select(tl.subject_id).where(tl.subject_type == "asset", tl.status == "approved", tl.subject_id.in_(part))))
                certified.update(r[0] for r in c.execute(select(cs.subject_id).where(cs.subject_type == "asset", cs.state == "certified", cs.subject_id.in_(part))))
                for aid, k in c.execute(select(g.asset_id, func.count()).where(g.status == "suggested", g.asset_id.in_(part)).group_by(g.asset_id)):
                    pending[aid] = pending.get(aid, 0) + int(k)
                # an approved label on the dataset itself or on one of its columns makes it classified
                classified.update(r[0] for r in c.execute(select(g.asset_id).where(g.status == "approved", g.asset_id.in_(part))))
                classified.update(r[0] for r in c.execute(select(a.parent_id).join(gov_class_assignments, g.asset_id == a.id).where(g.status == "approved", a.parent_id.in_(part))))
                # suggestions on columns count for their table
                for pid, k in c.execute(select(a.parent_id, func.count()).join(gov_class_assignments, g.asset_id == a.id).where(g.status == "suggested", a.parent_id.in_(part)).group_by(a.parent_id)):
                    pending[pid] = pending.get(pid, 0) + int(k)

        def pct(k: int) -> float:
            return round(100.0 * k / n, 1)

        described = sum(1 for r in rows if len((r["description"] or "").strip()) >= 10)
        unowned = [r for r in rows if r["id"] not in owned][:10]
        unreviewed = sorted((r for r in rows if pending.get(r["id"])), key=lambda r: (-pending[r["id"]], r["name"]))[:10]
        return {"coverage": {"datasets": n, "owner_pct": pct(len(owned)), "description_pct": pct(described), "domain_pct": pct(len(with_domain)), "term_pct": pct(len(termed)),
                             "classified_pct": pct(len(classified)),
                             "classification_reviewed_pct": pct(sum(1 for r in rows if r["id"] in classified and not pending.get(r["id"]))), "datasets_with_pending_suggestions": sum(1 for r in rows if pending.get(r["id"])),
                             "certified_pct": pct(len(certified)), "sampled": sampled},
                "top_unowned": [compact_asset(r) for r in unowned], "top_unclassified": [{**compact_asset(r), "pending_suggestions": pending[r["id"]]} for r in unreviewed]}

    # ------------------------------------------------------------------------------------------------------------------------ events and background work
    def on_event(self, evt: dict[str, Any]) -> None:
        """Event-bus subscriber: append a ``gov_signals`` row for the events governance reacts to. Cheap; never raises."""
        try:
            etype = str(evt.get("type") or "")
            if etype == "intelligence.scan.finished":
                p = evt.get("payload") or {}
                if p.get("status") not in ("succeeded", "partial"):
                    return
                kind, payload = "scan_finished", {"scan_id": p.get("scan_id"), "source_id": p.get("source_id"), "mode": p.get("mode"), "status": p.get("status")}
            elif etype == "dataset.schema_changed":
                kind, payload = "schema_changed", {"table": (evt.get("payload") or {}).get("table")}
            else:
                return
            if not self.tables_available():
                return
            with self.db.begin() as c:
                if kind == "schema_changed" and c.execute(select(gov_signals.c.seq).where(gov_signals.c.kind == "schema_changed", gov_signals.c.status == "pending").limit(1)).first():
                    return
                c.execute(insert(gov_signals).values(kind=kind, payload=payload, status="pending", attempts=0, created_at=now_iso()))
        except Exception as exc:  # noqa: BLE001 - the publisher must never notice
            self.last_error = f"signal: {type(exc).__name__}: {str(exc)[:200]}"
            log.debug("could not record a governance signal: %s", exc)

    def interval(self) -> float:
        """Seconds between loop passes (setting ``governance.tick_seconds``)."""
        try:
            return max(1.0, float(self._base.setting("governance.tick_seconds", 30)))
        except (TypeError, ValueError):
            return 30.0

    def tick(self) -> dict[str, Any]:
        """One pass of the background work (see the module docstring). Safe to call from several replicas: signal rows are claimed with a conditional update."""
        if not self.tables_available(force=not self._tables_ok):
            return {"skipped": "governance tables are missing"}
        with self._lock:
            result: dict[str, Any] = {"signals": 0, "invalidated": 0, "scans": 0, "evaluated": 0}
            try:
                t = gov_signals.c
                with self.db.connect() as c:
                    pending = fetch_all(c, select(gov_signals).where(t.status == "pending").order_by(t.seq).limit(SIGNAL_BATCH))
                for sig in pending:
                    with self.db.begin() as c:
                        claimed = c.execute(update(gov_signals).where(t.seq == sig["seq"], t.status == "pending").values(status="working", attempts=sig["attempts"] + 1)).rowcount
                    if not claimed:
                        continue
                    try:
                        self._process(sig, result)
                        status, err = "done", None
                        self.stats["signals_done"] += 1
                    except Exception as exc:  # noqa: BLE001
                        log.exception("governance signal %s failed", sig["seq"])
                        err = f"{type(exc).__name__}: {str(exc)[:300]}"
                        status = "failed" if sig["attempts"] + 1 >= MAX_SIGNAL_ATTEMPTS else "pending"
                        self.stats["signals_failed"] += 1
                        self.last_error = err
                    with self.db.begin() as c:
                        c.execute(update(gov_signals).where(t.seq == sig["seq"]).values(status=status, error=err, processed_at=now_iso() if status != "pending" else None))
                    result["signals"] += 1
                now = time.monotonic()
                if now - self._last_sweep >= SWEEP_SECONDS or now < self._schema_until:
                    self._last_sweep = now
                    result["invalidated"] += len(self.certification.invalidate_changed())
                if now - self._last_purge >= 3600:
                    self._last_purge = now
                    self.policies.purge()
                    self._purge_signals()
            except Exception as exc:  # noqa: BLE001
                log.exception("governance tick failed")
                self.last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
                result["error"] = self.last_error
            self.stats["ticks"] += 1
            self.stats["invalidated"] += result["invalidated"]
            self.last_tick_at = now_iso()
            return result

    def _process(self, sig: dict[str, Any], result: dict[str, Any]) -> None:
        kind, payload = sig["kind"], sig["payload"] or {}
        if kind == "scan_finished":
            if self._base.setting("governance.auto_classify_on_scan", True):
                self.classifications.scan_assets(scan_id=payload.get("scan_id"), source_id=payload.get("source_id"))
                result["scans"] += 1
            self.propagation.invalidate()
            result["invalidated"] += len(self.certification.invalidate_changed())
            if self._base.setting("governance.evaluate_policies_on_scan", True):
                result["evaluated"] += len(self.policies.evaluate_all(None))
        elif kind == "schema_changed":
            self._schema_until = time.monotonic() + SCHEMA_WINDOW_SECONDS
            result["invalidated"] += len(self.certification.invalidate_changed())
        else:
            raise ValueError(f"unknown signal kind '{kind}'")

    def _purge_signals(self) -> None:
        cutoff = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self.db.begin() as c:
            c.execute(gov_signals.delete().where(gov_signals.c.status.in_(("done", "failed")), gov_signals.c.processed_at < cutoff))

    # ------------------------------------------------------------------------------------------------------------------------ health
    def health_labels(self) -> dict[str, Any]:
        """Small diagnostic block (also used in the summary)."""
        pending = failed = 0
        if self._tables_ok:
            try:
                with self.db.connect() as c:
                    for st, n in c.execute(select(gov_signals.c.status, func.count()).group_by(gov_signals.c.status)):
                        if st in ("pending", "working"):
                            pending += int(n)
                        elif st == "failed":
                            failed += int(n)
            except SQLAlchemyError:
                pass
        return {"tables_available": self._tables_ok, "missing_tables": self._missing, "signals_pending": pending, "signals_failed": failed, "last_tick_at": self.last_tick_at,
                "last_error": self.last_error, **self.stats}

    def health(self) -> ComponentHealth:
        """The ``intelligence-governance`` health component (database only)."""
        ok = self.tables_available(force=True)
        labels = self.health_labels()
        if not ok:
            return ComponentHealth("intelligence-governance", Status.DEGRADED, "governance tables are missing: " + ", ".join(self._missing[:5]) + " (run 'olcf db migrate')", category="data", labels=labels)
        problems = []
        if labels["signals_failed"]:
            problems.append(f"{labels['signals_failed']} governance signal(s) failed")
        if labels["last_error"]:
            problems.append(f"last error: {labels['last_error']}")
        detail = f"governance (observe only); {labels['signals_pending']} signal(s) pending"
        return ComponentHealth("intelligence-governance", Status.DEGRADED if problems else Status.OK, detail + ("; " + "; ".join(problems) if problems else ""), category="data", labels=labels)


__all__ = ["GovernanceService"]
