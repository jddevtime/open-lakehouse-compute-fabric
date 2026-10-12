# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Governance quality score: 0..100, explainable, configurable.

An asset's score is a weighted mean of up to eight **components**, each a fraction 0..1 of its own maximum:

``documentation``     a description of at least 10 characters (1), a shorter one (0.5), none (0).
``ownership``         an effective owner, direct or inherited (1); only a steward / custodian / contact (0.5); nobody (0).
``classification``    share of the classification rows on the asset and its columns that were decided (approved or rejected) rather than left as suggestions. Applies only where
                      there is classification activity at all.
``freshness``         age of the newest known write against ``governance.freshness_hours``: within the limit 1, linearly down to 0 at three times the limit. Applies only where a write
                      time is known (Delta last-modified or an observed write).
``certification``     certified 1, draft 0.5, deprecated or none 0.
``lineage``           the asset (or, for a table, one of its columns) appears in recorded data-flow lineage.
``violations``        1 minus the weight of open / acknowledged policy violations (critical 1.0, high 0.6, medium 0.3, low 0.1), never below 0. Violations of ``quality_min`` policies are left out so
                      that reporting a low score cannot lower it.
``schema_stability``  days since the last column change against ``governance.schema_stability_days``. Applies only to assets with a recorded schema history.

A component that does not apply to the asset type, or has no data, is ``applicable: false`` and its weight leaves the normalisation, so the ``points`` of the applicable components add up
to ``own_score`` exactly (``points = 100 * weight / sum of applicable weights * fraction``, rounded to two decimals). Weights are the settings ``governance.quality_weight.<component>``.

**Roll-up.** A table-like asset with columns blends the mean score of (up to :data:`MAX_ROLLUP_COLUMNS` of) its columns into its own: ``score = own * (1 - w) + mean * w`` with
``w = governance.quality_column_rollup_weight``; ``rollup.sampled`` says when columns were left out. A data product blends the mean of the scores of all its members into its own
three components (documentation, ownership, certification) with ``governance.quality_product_member_weight``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select

from olcf.core.timeutil import from_iso, now_iso
from olcf.db.util import fetch_all
from olcf.intelligence import model as M
from olcf.intelligence.governance.common import Base, chunks, compact_asset
from olcf.intelligence.governance.schema import gov_class_assignments, gov_policies, gov_violations
from olcf.intelligence.schema import asset_versions, assets, edges
from olcf.security.rbac import Principal

DEFAULT_WEIGHTS: dict[str, float] = {"documentation": 20.0, "ownership": 20.0, "classification": 10.0, "freshness": 15.0, "certification": 10.0, "lineage": 10.0, "violations": 10.0,
                                     "schema_stability": 5.0}
COMPONENTS = tuple(DEFAULT_WEIGHTS)
DATA_TYPES = frozenset({"table", "view", "model_table"})
OWNED_TYPES = frozenset({"table", "view", "model_table", "semantic_model", "report", "dashboard"})
DOC_TYPES = OWNED_TYPES | {"column", "measure"}
LINEAGE_TYPES = DOC_TYPES
SCHEMA_TYPES = frozenset({"table", "view", "model_table", "semantic_model"})
VIOLATION_WEIGHT = {"critical": 1.0, "high": 0.6, "medium": 0.3, "low": 0.1}
MAX_ROLLUP_COLUMNS = 300
MIN_DESCRIPTION = 10


class QualityManager(Base):
    """Computes the quality score (see the module docstring); results are computed on demand from current data, never stored."""

    # ------------------------------------------------------------------------------------------------------------------------ settings
    def weights(self) -> dict[str, float]:
        """Effective component weights (non-numeric or negative settings fall back to the default / zero)."""
        out: dict[str, float] = {}
        for name, default in DEFAULT_WEIGHTS.items():
            raw = self.setting(f"governance.quality_weight.{name}", default)
            try:
                out[name] = max(0.0, float(raw))
            except (TypeError, ValueError):
                out[name] = default
        return out

    def _fraction_setting(self, key: str, default: float) -> float:
        try:
            return min(1.0, max(0.0, float(self.setting(key, default))))
        except (TypeError, ValueError):
            return default

    # ------------------------------------------------------------------------------------------------------------------------ data loading
    def last_writes(self, rows: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
        """``{asset_id: (time, source)}``: the newest known write, from the Delta log (``attrs.last_modified``) or an observed ``WRITES_TO`` event; assets with none are absent."""
        out: dict[str, tuple[str, str]] = {}
        for r in rows:
            lm = (r.get("attrs") or {}).get("last_modified")
            if lm:
                out[r["id"]] = (str(lm), "table modified time")
        ids = [r["id"] for r in rows]
        e = edges.c
        with self.db.connect() as c:
            for part in chunks(ids):
                for dst, ts in c.execute(select(e.dst_id, func.max(e.last_verified_at)).where(e.type == "WRITES_TO", e.status == "active", e.dst_id.in_(part), e.last_verified_at.is_not(None))
                                         .group_by(e.dst_id)):
                    if ts and (dst not in out or str(ts) > out[dst][0]):
                        out[dst] = (str(ts), "observed write")
        return out

    def _columns(self, table_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {i: [] for i in table_ids}
        a = assets.c
        with self.db.connect() as c:
            for part in chunks(table_ids, 100):
                for r in fetch_all(c, select(a).where(a.parent_id.in_(part), a.type == "column", a.status == "active").order_by(a.parent_id, a.name)):
                    if len(out[r["parent_id"]]) < MAX_ROLLUP_COLUMNS:
                        out[r["parent_id"]].append(r)
                    else:
                        out[r["parent_id"]].append({"_overflow": True})
        return out

    def _load(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        ids = [r["id"] for r in rows]
        owned = [r["id"] for r in rows if r["type"] in OWNED_TYPES]
        data: dict[str, Any] = {"owners": self.gov.owners.effective(owned), "cert": self.gov.certification.states("asset", owned),
                                "writes": self.last_writes([r for r in rows if r["type"] in DATA_TYPES])}
        g = gov_class_assignments.c
        decided: dict[str, int] = {}
        pending: dict[str, int] = {}
        with self.db.connect() as c:
            for part in chunks(ids):
                for aid, st, n in c.execute(select(g.asset_id, g.status, func.count()).where(g.asset_id.in_(part)).group_by(g.asset_id, g.status)):
                    (pending if st == "suggested" else decided)[aid] = (pending if st == "suggested" else decided).get(aid, 0) + int(n)
        data["cls_decided"], data["cls_pending"] = decided, pending
        flowing = {t for t in M.FLOW_EDGE_TYPES}
        e = edges.c
        linked: set[str] = set()
        with self.db.connect() as c:
            for part in chunks(ids):
                for col in (e.src_id, e.dst_id):
                    linked.update(r[0] for r in c.execute(select(col).where(col.in_(part), e.status == "active", e.type.in_(sorted(flowing))).distinct()))
        data["linked"] = linked
        v, p = gov_violations.c, gov_policies.c
        viol: dict[str, dict[str, int]] = {}
        with self.db.connect() as c:
            for part in chunks(ids):
                stmt = (select(v.subject_id, v.severity, func.count()).join(gov_policies, gov_policies.c.id == v.policy_id)
                        .where(v.subject_type == "asset", v.subject_id.in_(part), v.status.in_(("open", "acknowledged"))).group_by(v.subject_id, v.severity))
                stmt = stmt.where(p.rule_type != "quality_min")      # a low score is reported by quality_min policies; it must not lower itself
                for sid, sev, n in c.execute(stmt):
                    viol.setdefault(sid, {})[sev] = int(n)
        data["violations"] = viol
        # schema history: the newest column change after the first recorded version
        change: dict[str, str] = {}
        has_history: set[str] = set()
        t = asset_versions.c
        with self.db.connect() as c:
            for part in chunks([r["id"] for r in rows if r["type"] in SCHEMA_TYPES]):
                firsts = {aid: fid for aid, fid in c.execute(select(t.asset_id, func.min(t.id)).where(t.asset_id.in_(part)).group_by(t.asset_id))}
                has_history.update(firsts)
                for r in c.execute(select(t.asset_id, t.id, t.changes, t.created_at).where(t.asset_id.in_(part)).order_by(t.id.desc())):
                    aid, vid, ch, at = r
                    if aid in change or vid == firsts.get(aid):
                        continue
                    if any(str(k).startswith("columns_") for k in (ch or {})):
                        change[aid] = str(at)
        data["schema_changed"], data["has_history"] = change, has_history
        return data

    # ------------------------------------------------------------------------------------------------------------------------ components
    def _components(self, row: dict[str, Any], d: dict[str, Any]) -> dict[str, tuple[float | None, list[str]]]:
        """``{component: (fraction or None when not applicable, [reasons])}`` for one asset."""
        rid, typ = row["id"], row["type"]
        out: dict[str, tuple[float | None, list[str]]] = {}
        if typ in DOC_TYPES:
            n = len((row["description"] or "").strip())
            out["documentation"] = (1.0 if n >= MIN_DESCRIPTION else (0.5 if n else 0.0), [] if n >= MIN_DESCRIPTION else ["no description" if n == 0 else f"the description is only {n} characters (at least {MIN_DESCRIPTION} expected)"])
        if typ in OWNED_TYPES:
            owners = d["owners"].get(rid, [])
            if any(o["role"] == "owner" for o in owners):
                out["ownership"] = (1.0, [])
            elif owners:
                out["ownership"] = (0.5, ["there is a steward / custodian / contact but no owner"])
            else:
                out["ownership"] = (0.0, ["no owner, direct or inherited"])
        dec, pen = d["cls_decided"].get(rid, 0) + d.get("child_decided", {}).get(rid, 0), d["cls_pending"].get(rid, 0) + d.get("child_pending", {}).get(rid, 0)
        if typ in DATA_TYPES or typ == "column":
            if dec + pen:
                out["classification"] = (dec / (dec + pen), [] if not pen else [f"{pen} classification suggestion(s) are waiting for review"])
        if typ in DATA_TYPES:
            w = d["writes"].get(rid)
            if w:
                try:
                    age = (datetime.now(UTC) - from_iso(w[0])).total_seconds() / 3600
                    hours = max(0.001, float(self.setting("governance.freshness_hours", 24)))
                    frac = 1.0 if age <= hours else max(0.0, 1.0 - (age - hours) / (2 * hours))
                    out["freshness"] = (frac, [] if frac >= 1.0 else [f"last written {age:.1f} hours ago (target {hours:g} hours; the score reaches 0 at {3 * hours:g} hours)"])
                except Exception:  # noqa: BLE001 - an unparsable time means "unknown"
                    pass
        if typ in OWNED_TYPES:
            st = d["cert"].get(rid, "none")
            out["certification"] = ({"certified": 1.0, "draft": 0.5}.get(st, 0.0), [] if st == "certified" else [f"certification is '{st}'"])
        if typ in LINEAGE_TYPES:
            linked = rid in d["linked"] or rid in d.get("child_linked", set())
            out["lineage"] = (1.0 if linked else 0.0, [] if linked else ["no data-flow lineage is recorded"])
        if typ in DOC_TYPES:
            vs = d["violations"].get(rid, {})
            loss = min(1.0, sum(VIOLATION_WEIGHT.get(s, 0.3) * n for s, n in vs.items()))
            out["violations"] = (1.0 - loss, [f"{n} open {s} violation(s)" for s, n in sorted(vs.items(), key=lambda x: -VIOLATION_WEIGHT.get(x[0], 0))])
        if typ in SCHEMA_TYPES and rid in d["has_history"]:
            days_window = max(1.0, float(self.setting("governance.schema_stability_days", 30)))
            changed = d["schema_changed"].get(rid)
            if changed is None:
                out["schema_stability"] = (1.0, [])
            else:
                days = (datetime.now(UTC) - from_iso(changed)).total_seconds() / 86400
                frac = min(1.0, max(0.0, days / days_window))
                out["schema_stability"] = (frac, [] if frac >= 1.0 else [f"the schema changed {days:.1f} days ago (stable for {days_window:g} days earns full marks)"])
        return out

    def _finish(self, row: dict[str, Any], comps: dict[str, tuple[float | None, list[str]]], weights: dict[str, float]) -> dict[str, Any]:
        applicable = {k: v for k, (v, _) in comps.items() if v is not None and weights.get(k, 0) > 0}
        total = sum(weights[k] for k in applicable)
        shaped = []
        score = 0.0
        for name in COMPONENTS:
            frac, reasons = comps.get(name, (None, []))
            w = weights.get(name, 0.0)
            on = name in applicable and total > 0
            maxp = round(100.0 * w / total, 2) if on else 0.0
            pts = round(maxp * (frac or 0.0), 2) if on else 0.0
            score += pts
            lost = round(maxp - pts, 2)
            rs: list[dict[str, Any]] = []
            if on and lost > 0 and reasons:
                share = round(lost / len(reasons), 2)
                rs = [{"points_lost": share, "reason": x} for x in reasons]
                rs[-1]["points_lost"] = round(lost - share * (len(reasons) - 1), 2)
            shaped.append({"name": name, "weight": w, "applicable": on, "fraction": None if not on else round(frac or 0.0, 4), "points": pts, "max_points": maxp, "reasons": rs})
        return {"asset_id": row["id"], "type": row["type"], "own_score": round(score, 2) if total > 0 else None, "components": shaped}

    # ------------------------------------------------------------------------------------------------------------------------ public computation
    def scores(self, asset_ids: list[str], *, rollup: bool = True) -> dict[str, dict[str, Any]]:
        """Quality of the given assets (``score`` ``None`` for an asset with no applicable component); unknown ids are absent. Bulk: a fixed number of queries per page."""
        rows = list(self.intel.graph.asset_rows(list(dict.fromkeys(asset_ids))).values())
        rows = [r for r in rows if r["status"] != "removed"]
        if not rows:
            return {}
        weights = self.weights()
        cw = self._fraction_setting("governance.quality_column_rollup_weight", 0.3)
        tables = [r["id"] for r in rows if r["type"] in DATA_TYPES]
        children = self._columns(tables) if tables and rollup else {}
        col_rows = [c for cs in children.values() for c in cs if "_overflow" not in c]
        everyone = rows + col_rows
        data = self._load(everyone)
        # classification activity on columns counts for the table; lineage on a column counts for the table
        child_dec: dict[str, int] = {}
        child_pen: dict[str, int] = {}
        child_linked: set[str] = set()
        for tid, cs in children.items():
            for c in cs:
                if "_overflow" in c:
                    continue
                child_dec[tid] = child_dec.get(tid, 0) + data["cls_decided"].get(c["id"], 0)
                child_pen[tid] = child_pen.get(tid, 0) + data["cls_pending"].get(c["id"], 0)
                if c["id"] in data["linked"]:
                    child_linked.add(tid)
        data["child_decided"], data["child_pending"], data["child_linked"] = child_dec, child_pen, child_linked
        out: dict[str, dict[str, Any]] = {}
        col_score: dict[str, float | None] = {}
        for c in col_rows:
            col_score[c["id"]] = self._finish(c, self._components(c, data), weights)["own_score"]
        now = now_iso()
        for r in rows:
            res = self._finish(r, self._components(r, data), weights)
            own = res["own_score"]
            roll = None
            score = own
            cs = children.get(r["id"], [])
            real = [c for c in cs if "_overflow" not in c]
            if real:
                vals = [v for v in (col_score.get(c["id"]) for c in real) if v is not None]
                if vals:
                    mean = round(sum(vals) / len(vals), 2)
                    roll = {"columns": {"count": len(vals), "mean": mean, "sampled": len(real) != len(cs)}, "own_score": own, "column_weight": cw}
                    if own is not None:
                        score = round(own * (1 - cw) + mean * cw, 2)
                    else:
                        score = mean
            out[r["id"]] = {**res, "score": score, "rollup": roll, "weights": weights, "computed_at": now}
        return out

    def for_asset(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """``GET /governance/assets/{id}/quality``."""
        row = self.asset(principal, asset_id)
        res = self.scores([row["id"]]).get(row["id"])
        if res is None:
            return {"asset_id": row["id"], "score": None, "components": [], "rollup": None, "own_score": None, "weights": self.weights(), "computed_at": now_iso(),
                    "note": "a removed asset has no quality score"}
        if res["score"] is None:
            res["note"] = f"quality does not apply to assets of type '{row['type']}'"
        return res

    # ------------------------------------------------------------------------------------------------------------------------ products
    def _product_own(self, product: dict[str, Any], weights: dict[str, float]) -> dict[str, Any]:
        owners = self.gov.owners.direct("product", [product["id"]]).get(product["id"], [])
        cert = self.gov.certification.states("product", [product["id"]]).get(product["id"], "none")
        n = len((product["description"] or "").strip())
        comps: dict[str, tuple[float | None, list[str]]] = {
            "documentation": (1.0 if n >= MIN_DESCRIPTION else (0.5 if n else 0.0), [] if n >= MIN_DESCRIPTION else ["no description" if n == 0 else "the description is very short"]),
            "ownership": ((1.0 if any(o["role"] == "owner" for o in owners) else (0.5 if owners else 0.0)), [] if any(o["role"] == "owner" for o in owners) else ["the product has no owner"]),
            "certification": ({"certified": 1.0, "draft": 0.5}.get(cert, 0.0), [] if cert == "certified" else [f"certification is '{cert}'"]),
        }
        return self._finish({"id": product["id"], "type": "product"}, comps, weights)

    def product_detail(self, product_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Quality of data products: own components blended with the mean score of all members. Bulk over products and members."""
        from olcf.intelligence.governance.schema import gov_products

        out: dict[str, dict[str, Any]] = {}
        if not product_ids:
            return out
        weights = self.weights()
        mw = self._fraction_setting("governance.quality_product_member_weight", 0.7)
        with self.db.connect() as c:
            prods = {}
            for part in chunks(list(dict.fromkeys(product_ids))):
                prods.update({p["id"]: p for p in fetch_all(c, select(gov_products).where(gov_products.c.id.in_(part)))})
        members = {pid: self.gov.products.member_ids(pid) for pid in prods}
        everyone = sorted({a for m in members.values() for a in m})
        mscores = self.scores(everyone, rollup=True) if everyone else {}
        for pid, p in prods.items():
            own = self._product_own(p, weights)
            vals = [mscores[m]["score"] for m in members[pid] if m in mscores and mscores[m]["score"] is not None]
            mean = round(sum(vals) / len(vals), 2) if vals else None
            score = own["own_score"]
            if mean is not None:
                score = round(own["own_score"] * (1 - mw) + mean * mw, 2) if own["own_score"] is not None else mean
            out[pid] = {"product_id": pid, "score": score, "own_score": own["own_score"], "product_components": own["components"], "member_mean": mean, "member_weight": mw if mean is not None else 0.0,
                        "members_scored": len(vals), "members_total": len(members[pid]), "_scores": {m: mscores[m]["score"] for m in members[pid] if m in mscores}}
        return out

    def product_scores(self, product_ids: list[str]) -> dict[str, float | None]:
        """``{product id: score}`` (``None`` when nothing could be scored)."""
        return {k: v["score"] for k, v in self.product_detail(product_ids).items()}

    def product(self, principal: Principal, product_id: str) -> dict[str, Any]:
        """``GET /governance/products/{id}/quality``: the score uses *all* members; names are listed only for members the caller can see."""
        self.org_reader(principal)
        self.gov.products.row(product_id)
        res = self.product_detail([product_id])[product_id]
        scored = res.pop("_scores")
        rows = self.visible_rows(principal, list(scored))
        members = [{"asset": compact_asset(rows[m]), "score": s} for m, s in scored.items() if m in rows]
        members.sort(key=lambda x: (x["score"] is None, x["score"] if x["score"] is not None else 0, x["asset"]["name"]))
        res["members"] = members
        res["hidden_members"] = res["members_total"] - len(members)
        res["weights"] = self.weights()
        res["computed_at"] = now_iso()
        return res


__all__ = ["COMPONENTS", "DEFAULT_WEIGHTS", "QualityManager"]
