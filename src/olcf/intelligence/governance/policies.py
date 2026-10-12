# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Policies (OBSERVE mode only) and their violations.

A policy is a *rule type* plus validated parameters, a severity and a mode. The only mode is ``observe``: evaluating a policy **reports** violations, it never blocks, rewrites or
delays anything. ``mode: "enforce"`` is refused with ``Unsupported`` and a remediation, and nothing in the compute, SQL or pipeline path calls governance.

Rule types (closed set, see :data:`RULE_TYPES`): ``classification_flow``, ``required_owner``, ``required_description``, ``required_classification``, ``certified_sources_only``,
``freshness`` and ``quality_min``. Parameters are validated when the policy is saved so a stored policy always evaluates.

**Violations** (``vio_``) are idempotent: one row per ``(policy, subject, discriminator)``. Evaluating again keeps the id and ``first_seen_at`` of a finding that is still true and
moves ``last_seen_at``; a finding that is no longer true becomes ``resolved``; one that returns is ``open`` again (``reopen_count`` +1). Statuses: ``open``, ``acknowledged`` (seen, still
reported), ``excepted`` (a change request granted an exception, optionally until a time) and ``resolved``. An exception that expired while the finding is still true reopens it.

Conservative semantics: a rule reports only what it can prove from the data it has. An asset whose freshness (or quality) cannot be determined is counted as ``unknown`` in the result
and never reported; a truncated evaluation (more assets than ``governance.max_evaluation_assets``) never resolves old violations. Messages name only the subject itself; other assets
appear in ``evidence`` as ``{"asset_id", "name", ...}`` objects that are replaced by ``{"hidden": true}`` for callers who cannot see them.
"""

from __future__ import annotations

import builtins
import fnmatch
import hashlib
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, insert, or_, select, update

from olcf.core.errors import BadRequest, Conflict, NotFound, Unavailable, Unsupported
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, new_etag
from olcf.intelligence import model as M
from olcf.intelligence.governance.common import (
    SEVERITIES,
    Base,
    chunks,
    clean_name,
    clean_str_list,
    clean_text,
    compact_asset,
    name_key,
)
from olcf.intelligence.governance.glossary import raise_precondition
from olcf.intelligence.governance.schema import gov_policies, gov_violations
from olcf.intelligence.governance.selectors import Candidates, Matcher, clean_selector, row_tags
from olcf.security.rbac import Principal

MODES = ("observe",)
VIOLATION_STATUSES = ("open", "acknowledged", "excepted", "resolved")
DEFAULT_OWNED_TYPES = ("table", "view", "model_table", "semantic_model", "report", "dashboard")
DATA_TARGET_TYPES = ("table", "view", "model_table", "semantic_model", "report", "dashboard")
SOURCE_TYPES = ("table", "view", "model_table", "semantic_model")
MAX_EVENTS_PER_RUN = 200
MAX_EVIDENCE_ITEMS = 20
ENFORCE_REMEDIATION = ("OLCF governance observes and reports; it never blocks or changes execution. Create the policy with mode 'observe' and act on its violations "
                       "(review them in the governance UI, or subscribe to the governance.violation.opened webhook event).")

RULE_TYPES: dict[str, dict[str, Any]] = {
    "classification_flow": {
        "description": "Data carrying a classification must not reach assets matching 'target' (unless the target has an allowed label or the flow is masked). Only proven exposures count by default.",
        "params": {"classification": "required: classification id or name", "target": "selector of the assets that must not receive the data (default: tables, views, semantic models, reports, dashboards)",
                   "allowed_labels": "optional list of classification names / ids; a target carrying one of them is fine", "masking_tag": "tag that marks a masked asset on the path (default 'masked'; '' disables)",
                   "include_possible": "optional bool (default false): also report possible exposures that lineage does not prove"},
    },
    "required_owner": {
        "description": "Assets must have an effective owner (direct or inherited from a parent asset).",
        "params": {"types": f"asset types (default {list(DEFAULT_OWNED_TYPES)})", "roles": "owner roles that satisfy the rule (default ['owner'])", "selector": "optional selector"},
    },
    "required_description": {
        "description": "Assets must have a description of at least min_length characters.",
        "params": {"types": f"asset types (default {list(DEFAULT_OWNED_TYPES)})", "min_length": "integer 1..2000 (default 10)", "selector": "optional selector"},
    },
    "required_classification": {
        "description": "Columns whose name matches one of the patterns must carry an approved classification (direct or proven inherited).",
        "params": {"name_patterns": "required: case-insensitive globs such as ['*email*', 'ssn']", "allowed_labels": "optional: only these labels satisfy the rule", "selector": "optional selector (types are always 'column')"},
    },
    "certified_sources_only": {
        "description": "Reports / dashboards must be built on certified sources (a source is certified itself or belongs to a certified data product).",
        "params": {"target_types": "default ['report', 'dashboard']", "source_types": f"default {list(SOURCE_TYPES)}", "include_products": "bool (default true): membership of a certified data product counts",
                   "depth": "upstream dataset hops 1..10 (default 5)", "selector": "optional selector of the targets"},
    },
    "freshness": {
        "description": "Assets whose newest known write is older than max_age_hours. Assets with no known write time are counted as unknown, never reported.",
        "params": {"max_age_hours": "required number > 0", "types": "default ['table']", "selector": "optional selector"},
    },
    "quality_min": {
        "description": "Assets (and optionally data products) whose governance quality score is below min_score.",
        "params": {"min_score": "required number 0..100", "types": "default ['table', 'view']", "include_products": "bool (default false)", "selector": "optional selector"},
    },
}


@dataclass
class Finding:
    """One violation found by a rule."""

    subject_type: str
    subject_id: str
    workspace_id: str | None
    message: str
    evidence: dict[str, Any]
    disc: str = ""

    @property
    def key(self) -> str:
        """Stable identity of the finding within a policy."""
        return hashlib.sha256(f"{self.subject_type}|{self.subject_id}|{self.disc}".encode()).hexdigest()


@dataclass
class Outcome:
    """What a rule evaluation produced."""

    findings: builtins.list[Finding] = field(default_factory=list)
    evaluated: int = 0
    unknown: int = 0
    truncated: bool = False
    notes: builtins.list[str] = field(default_factory=list)


def _asset_ev(row: dict[str, Any]) -> dict[str, Any]:
    return {"asset_id": row["id"], "name": row["name"], "type": row["type"]}


class PolicyManager(Base):
    """Policy CRUD, rule evaluation and the violation lifecycle (see the module docstring)."""

    # ------------------------------------------------------------------------------------------------------------------------ validation
    def rule_types(self) -> builtins.list[dict[str, Any]]:
        """The closed set of rule types with parameter documentation."""
        return [{"rule_type": k, **v} for k, v in RULE_TYPES.items()]

    def _types(self, value: Any, default: tuple[str, ...], what: str = "types") -> builtins.list[str]:
        types = clean_str_list(value, what, max_items=20, max_len=32) if value is not None else []
        bad = [t for t in types if t not in M.ASSET_TYPES]
        if bad:
            raise BadRequest(f"unknown asset type(s) {bad} in {what}")
        return types or list(default)

    @staticmethod
    def _bool(value: Any, what: str, default: bool) -> bool:
        if value is None:
            return default
        if not isinstance(value, bool):
            raise BadRequest(f"{what} must be true or false")
        return value

    @staticmethod
    def _number(value: Any, what: str, lo: float, hi: float | None = None, *, required: bool = True, default: float | None = None) -> float:
        if value is None:
            if required:
                raise BadRequest(f"{what} is required")
            assert default is not None
            return default
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise BadRequest(f"{what} must be a number")
        if value < lo or (hi is not None and value > hi):
            raise BadRequest(f"{what} must be between {lo:g} and {hi:g}" if hi is not None else f"{what} must be at least {lo:g}")
        return float(value)

    def validate_params(self, rule_type: str, params: Any) -> dict[str, Any]:
        """Validate and normalise the parameters of a rule type (``BadRequest`` with the supported keys otherwise)."""
        if rule_type not in RULE_TYPES:
            raise BadRequest(f"unknown rule_type '{rule_type}'", details={"supported": list(RULE_TYPES)})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise BadRequest("params must be an object")
        extra = set(params) - set(RULE_TYPES[rule_type]["params"])
        if extra:
            raise BadRequest(f"unknown parameter(s) {sorted(extra)} for {rule_type}", details={"supported": list(RULE_TYPES[rule_type]["params"])})
        out: dict[str, Any] = {}
        if rule_type == "classification_flow":
            if not params.get("classification"):
                raise BadRequest("classification_flow needs 'classification'")
            out["classification"] = self.gov.classifications.resolve(params["classification"])["id"]
            out["target"] = clean_selector(params.get("target"))
            labels = []
            for ref in clean_str_list(params.get("allowed_labels"), "allowed_labels", max_items=20, max_len=128):
                labels.append(self.gov.classifications.resolve(ref)["id"])
            out["allowed_labels"] = labels
            mt = params.get("masking_tag", "masked")
            if not isinstance(mt, str) or len(mt) > 64:
                raise BadRequest("masking_tag must be a string of at most 64 characters")
            out["masking_tag"] = mt.strip().lower()
            out["include_possible"] = self._bool(params.get("include_possible"), "include_possible", False)
        elif rule_type == "required_owner":
            out["types"] = self._types(params.get("types"), DEFAULT_OWNED_TYPES)
            roles = clean_str_list(params.get("roles"), "roles", max_items=4, max_len=16, lower=True) or ["owner"]
            from olcf.intelligence.governance.common import OWNER_ROLES

            bad = [r for r in roles if r not in OWNER_ROLES]
            if bad:
                raise BadRequest(f"unknown owner role(s) {bad}", details={"supported": list(OWNER_ROLES)})
            out["roles"] = roles
            out["selector"] = clean_selector(params.get("selector"))
        elif rule_type == "required_description":
            out["types"] = self._types(params.get("types"), DEFAULT_OWNED_TYPES)
            out["min_length"] = int(self._number(params.get("min_length"), "min_length", 1, 2000, required=False, default=10))
            out["selector"] = clean_selector(params.get("selector"))
        elif rule_type == "required_classification":
            pats = clean_str_list(params.get("name_patterns"), "name_patterns", max_items=50, max_len=128, lower=True)
            if not pats:
                raise BadRequest("required_classification needs 'name_patterns' (for example ['*email*', 'ssn'])")
            out["name_patterns"] = pats
            out["allowed_labels"] = [self.gov.classifications.resolve(r)["id"] for r in clean_str_list(params.get("allowed_labels"), "allowed_labels", max_items=20, max_len=128)]
            out["selector"] = clean_selector(params.get("selector"))
        elif rule_type == "certified_sources_only":
            out["target_types"] = self._types(params.get("target_types"), ("report", "dashboard"), "target_types")
            out["source_types"] = self._types(params.get("source_types"), SOURCE_TYPES, "source_types")
            out["include_products"] = self._bool(params.get("include_products"), "include_products", True)
            out["depth"] = int(self._number(params.get("depth"), "depth", 1, 10, required=False, default=5))
            out["selector"] = clean_selector(params.get("selector"))
        elif rule_type == "freshness":
            out["max_age_hours"] = self._number(params.get("max_age_hours"), "max_age_hours", 0.001)
            out["types"] = self._types(params.get("types"), ("table",))
            out["selector"] = clean_selector(params.get("selector"))
        else:  # quality_min
            out["min_score"] = self._number(params.get("min_score"), "min_score", 0, 100)
            out["types"] = self._types(params.get("types"), ("table", "view"))
            out["include_products"] = self._bool(params.get("include_products"), "include_products", False)
            out["selector"] = clean_selector(params.get("selector"))
        return out

    @staticmethod
    def _mode(value: Any) -> str:
        if value is None:
            return "observe"
        if value == "enforce":
            raise Unsupported("policy mode 'enforce' is not supported: governance policies only observe", remediation=ENFORCE_REMEDIATION)
        if value not in MODES:
            raise BadRequest(f"mode must be one of {list(MODES)}")
        return str(value)

    @staticmethod
    def _severity(value: Any) -> str:
        if value is None:
            return "medium"
        if value not in SEVERITIES:
            raise BadRequest(f"severity must be one of {list(SEVERITIES)}")
        return str(value)

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def row(self, policy_id: str) -> dict[str, Any]:
        """The raw policy row (``NotFound`` if absent)."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_policies).where(gov_policies.c.id == policy_id))
        if r is None:
            raise NotFound(f"policy '{policy_id}' does not exist")
        return r

    def _counts(self, policy_ids: builtins.list[str]) -> dict[str, int]:
        out: dict[str, int] = {}
        v = gov_violations.c
        with self.db.connect() as c:
            for part in chunks(policy_ids):
                for pid, n in c.execute(select(v.policy_id, func.count()).where(v.policy_id.in_(part), v.status.in_(("open", "acknowledged"))).group_by(v.policy_id)):
                    out[pid] = int(n)
        return out

    def _public(self, r: dict[str, Any], open_n: int) -> dict[str, Any]:
        stale = r["last_evaluated_at"] is None or r["updated_at"] > r["last_evaluated_at"]
        return {"id": r["id"], "name": r["name"], "description": r["description"], "rule_type": r["rule_type"], "params": r["params"], "severity": r["severity"], "mode": r["mode"],
                "enabled": bool(r["enabled"]), "open_violations": open_n, "last_evaluated_at": r["last_evaluated_at"], "last_result": r["last_result"] or {}, "needs_evaluation": bool(stale and r["enabled"]),
                "created_by": r["created_by"], "created_at": r["created_at"], "updated_at": r["updated_at"], "etag": r["etag"]}

    def list(self, principal: Principal, *, rule_type: str | None = None, enabled: bool | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Policies, oldest first."""
        self.org_reader(principal)
        t = gov_policies.c
        stmt = select(gov_policies)
        if rule_type:
            stmt = stmt.where(t.rule_type == rule_type)
        if enabled is not None:
            stmt = stmt.where(t.enabled == enabled)
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id > cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id).limit(limit + 1))
        counts = self._counts([r["id"] for r in rows[:limit]])
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return [self._public(r, counts.get(r["id"], 0)) for r in rows[:limit]], nxt

    def get(self, principal: Principal, policy_id: str) -> dict[str, Any]:
        """One policy."""
        self.org_reader(principal)
        r = self.row(policy_id)
        return self._public(r, self._counts([policy_id]).get(policy_id, 0))

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Create a policy (``governance.approve`` org-wide)."""
        self.org_approver(principal)
        extra = set(body) - {"name", "description", "rule_type", "params", "severity", "mode", "enabled"}
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}")
        mode = self._mode(body.get("mode"))
        name = clean_name(body.get("name"))
        rule_type = body.get("rule_type")
        if not isinstance(rule_type, str):
            raise BadRequest("rule_type is required", details={"supported": list(RULE_TYPES)})
        params = self.validate_params(rule_type, body.get("params"))
        now = now_iso()
        pid = new_id("plc")
        enabled = self._bool(body.get("enabled"), "enabled", True)
        with self.db.begin() as c:
            if fetch_one(c, select(gov_policies.c.id).where(gov_policies.c.name_key == name_key(name))):
                raise Conflict(f"a policy named '{name}' already exists")
            c.execute(insert(gov_policies).values(id=pid, name=name, name_key=name_key(name), description=clean_text(body.get("description"), "description", max_len=4000) or "", rule_type=rule_type,
                                                  params=params, severity=self._severity(body.get("severity")), mode=mode, enabled=enabled, etag=new_etag(), created_by=principal.id,
                                                  created_at=now, updated_at=now, last_result={}))
        self.audit(principal, "governance.policy_created", resource_type="policy", resource_id=pid, details={"name": name, "rule_type": rule_type})
        return self.get(principal, pid)

    def update(self, principal: Principal, policy_id: str, body: dict[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a policy (``rule_type`` is fixed)."""
        self.org_approver(principal)
        extra = set(body) - {"name", "description", "params", "severity", "mode", "enabled"}
        if extra:
            raise BadRequest(f"unknown or immutable field(s) {sorted(extra)}")
        cur = self.row(policy_id)
        self.check_etag(if_match, cur["etag"])
        vals: dict[str, Any] = {}
        if "mode" in body:
            vals["mode"] = self._mode(body["mode"])
        if "name" in body:
            vals["name"] = clean_name(body["name"])
            vals["name_key"] = name_key(vals["name"])
        if "description" in body:
            vals["description"] = clean_text(body["description"], "description", max_len=4000) or ""
        if "params" in body:
            vals["params"] = self.validate_params(cur["rule_type"], body["params"])
        if "severity" in body:
            vals["severity"] = self._severity(body["severity"])
        if "enabled" in body:
            vals["enabled"] = self._bool(body["enabled"], "enabled", True)
        if not vals:
            return self.get(principal, policy_id)
        vals.update(updated_at=now_iso(), etag=new_etag())
        t = gov_policies.c
        with self.db.begin() as c:
            if "name_key" in vals and fetch_one(c, select(t.id).where(t.name_key == vals["name_key"], t.id != policy_id)):
                raise Conflict(f"a policy named '{vals['name']}' already exists")
            if c.execute(update(gov_policies).where(t.id == policy_id, t.etag == cur["etag"]).values(**vals)).rowcount == 0:
                raise_precondition(self.row(policy_id)["etag"])
            if vals.get("enabled") is False and cur["enabled"]:
                c.execute(update(gov_violations).where(gov_violations.c.policy_id == policy_id, gov_violations.c.status != "resolved")
                          .values(status="resolved", resolved_at=now_iso(), etag=new_etag()))
        self.audit(principal, "governance.policy_updated", resource_type="policy", resource_id=policy_id, details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "name_key"))})
        return self.get(principal, policy_id)

    def delete(self, principal: Principal, policy_id: str, if_match: str | None = None) -> dict[str, Any]:
        """Delete a policy and its violations."""
        self.org_approver(principal)
        cur = self.row(policy_id)
        self.check_etag(if_match, cur["etag"])
        with self.db.begin() as c:
            n = c.execute(delete(gov_violations).where(gov_violations.c.policy_id == policy_id)).rowcount
            c.execute(delete(gov_policies).where(gov_policies.c.id == policy_id))
        self.audit(principal, "governance.policy_deleted", resource_type="policy", resource_id=policy_id, details={"name": cur["name"], "violations_deleted": n})
        return {"id": policy_id, "deleted": True, "violations_deleted": n}

    # ------------------------------------------------------------------------------------------------------------------------ rule evaluation
    def _sets(self, sel: dict[str, Any]) -> tuple[set[str] | None, set[str] | None]:
        inside = outside = None
        if sel.get("domain"):
            if self.gov.domains.resolve(sel["domain"]) is None:
                raise BadRequest(f"domain '{sel['domain']}' does not exist")
            inside = set(self.gov.domains.asset_ids_in(sel["domain"], limit=200_000))
        if sel.get("outside_domain"):
            if self.gov.domains.resolve(sel["outside_domain"]) is None:
                raise BadRequest(f"domain '{sel['outside_domain']}' does not exist")
            outside = set(self.gov.domains.asset_ids_in(sel["outside_domain"], limit=200_000))
        return inside, outside

    def _matcher(self, sel: dict[str, Any], default_types: tuple[str, ...] | builtins.list[str], *, force_types: bool = False) -> Matcher:
        inside, outside = self._sets(sel)
        return Matcher(sel, tuple(default_types), inside, outside, force_types=force_types)

    def _limit(self) -> int:
        return int(self.setting("governance.max_evaluation_assets", 20000))

    def _evaluate_rule(self, rule_type: str, params: dict[str, Any]) -> Outcome:
        fn = getattr(self, f"_rule_{rule_type}")
        out: Outcome = fn(params)
        return out

    def _rule_required_owner(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cand = Candidates(self.db, self._matcher(p["selector"], p["types"], force_types=not p["selector"].get("types")), limit=self._limit())
        for page in cand._iterate():
            eff = self.gov.owners.effective([r["id"] for r in page])
            for r in page:
                out.evaluated += 1
                if not any(o["role"] in p["roles"] for o in eff.get(r["id"], [])):
                    out.findings.append(Finding("asset", r["id"], r["workspace_id"], f"'{r['name']}' has no {' or '.join(p['roles'])} (direct or inherited)",
                                                {"roles_required": p["roles"], "effective_owners": [o["name"] for o in eff.get(r["id"], [])]}))
        out.truncated = cand.truncated
        return out

    def _rule_required_description(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cand = Candidates(self.db, self._matcher(p["selector"], p["types"], force_types=not p["selector"].get("types")), limit=self._limit())
        for page in cand._iterate():
            for r in page:
                out.evaluated += 1
                n = len((r["description"] or "").strip())
                if n < p["min_length"]:
                    out.findings.append(Finding("asset", r["id"], r["workspace_id"], f"'{r['name']}' has {'no description' if n == 0 else f'a description of only {n} characters'} (minimum {p['min_length']})",
                                                {"length": n, "min_length": p["min_length"], "description_source": r["description_source"]}))
        out.truncated = cand.truncated
        return out

    def _rule_required_classification(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        matcher = self._matcher(p["selector"], ("column",), force_types=True)
        cand = Candidates(self.db, matcher, name_globs=p["name_patterns"], limit=self._limit())
        allowed = set(p["allowed_labels"])
        cm = self.gov.classifications
        for page in cand._iterate():
            ids = [r["id"] for r in page]
            direct = cm.rows_for_assets(ids)
            suggested = cm.rows_for_assets(ids, ("suggested",))
            inherited = self.gov.propagation.inherited_for(ids, proven_only=True)
            parents = {r["parent_id"] for r in page if r["parent_id"]}
            prow = self.intel.graph.asset_rows(sorted(parents)) if parents else {}
            for r in page:
                out.evaluated += 1
                have = {a["class_id"] for a in direct.get(r["id"], [])} | {i.class_id for i in inherited.get(r["id"], [])}
                if (have & allowed) if allowed else have:
                    continue
                pat = next(g for g in p["name_patterns"] if fnmatch.fnmatchcase(r["name"].lower(), g))
                table = prow.get(r["parent_id"]) if r["parent_id"] else None
                label = f"column '{r['name']}'" + (f" of '{table['name']}'" if table else "")
                ev: dict[str, Any] = {"pattern": pat, "table": _asset_ev(table) if table else None, "allowed_labels": sorted(allowed)}
                if have:
                    ev["has_other_labels"] = sorted(have)
                if suggested.get(r["id"]):
                    ev["suggestions_waiting_for_review"] = len(suggested[r["id"]])
                out.findings.append(Finding("asset", r["id"], r["workspace_id"], f"{label} matches '{pat}' but has no approved classification" + (" from the allowed labels" if allowed else ""), ev))
        out.truncated = cand.truncated
        return out

    def _rule_freshness(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cand = Candidates(self.db, self._matcher(p["selector"], p["types"], force_types=not p["selector"].get("types")), limit=self._limit())
        now = datetime.now(UTC)
        for page in cand._iterate():
            last = self.gov.quality.last_writes(page)
            for r in page:
                got = last.get(r["id"])
                if not got:
                    out.unknown += 1
                    continue
                out.evaluated += 1
                try:
                    age = (now - from_iso(str(got[0]))).total_seconds() / 3600
                except Exception:  # noqa: BLE001 - an unparsable time is unknown, not a violation
                    out.unknown += 1
                    out.evaluated -= 1
                    continue
                if age > p["max_age_hours"]:
                    out.findings.append(Finding("asset", r["id"], r["workspace_id"], f"'{r['name']}' was last written {age:.1f} hours ago (limit {p['max_age_hours']:g})",
                                                {"last_write_at": got[0], "last_write_source": got[1], "age_hours": round(age, 2), "max_age_hours": p["max_age_hours"]}))
        out.truncated = cand.truncated
        return out

    def _rule_quality_min(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cand = Candidates(self.db, self._matcher(p["selector"], p["types"], force_types=not p["selector"].get("types")), limit=self._limit())
        for page in cand._iterate():
            scores = self.gov.quality.scores([r["id"] for r in page])
            for r in page:
                s = scores.get(r["id"])
                if s is None or s.get("score") is None:
                    out.unknown += 1
                    continue
                out.evaluated += 1
                if s["score"] < p["min_score"]:
                    worst = sorted((x for c in s["components"] for x in [(c["name"], rs) for rs in c["reasons"]]), key=lambda t: -t[1]["points_lost"])[:3]
                    out.findings.append(Finding("asset", r["id"], r["workspace_id"], f"'{r['name']}' has a quality score of {s['score']:g} (minimum {p['min_score']:g})",
                                                {"score": s["score"], "min_score": p["min_score"], "largest_losses": [{"component": n, **rs} for n, rs in worst],
                                                 "note": "quality scores ignore violations of quality_min policies, so a policy cannot lower the score it measures"}))
        out.truncated = cand.truncated
        if p["include_products"]:
            from olcf.intelligence.governance.schema import gov_products

            with self.db.connect() as c:
                prods = fetch_all(c, select(gov_products.c.id, gov_products.c.name).where(gov_products.c.status != "deprecated").limit(5000))
            ps = self.gov.quality.product_scores([x["id"] for x in prods])
            for prod in prods:
                sc = ps.get(prod["id"])
                if sc is None:
                    out.unknown += 1
                    continue
                out.evaluated += 1
                if sc < p["min_score"]:
                    out.findings.append(Finding("product", prod["id"], None, f"data product '{prod['name']}' has a quality score of {sc:g} (minimum {p['min_score']:g})",
                                                {"score": sc, "min_score": p["min_score"]}))
        return out

    def _carries(self, ids: builtins.list[str], labels: set[str]) -> set[str]:
        """Ids among ``ids`` that carry (directly approved, or proven inherited) one of ``labels``."""
        if not labels or not ids:
            return set()
        direct = self.gov.classifications.rows_for_assets(ids)
        inherited = self.gov.propagation.inherited_for(ids, proven_only=True)
        return {i for i in ids if {a["class_id"] for a in direct.get(i, [])} & labels or {x.class_id for x in inherited.get(i, [])} & labels}

    def _rule_classification_flow(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cls = self.gov.classifications.resolve(p["classification"])
        res = self.gov.propagation.result()
        out.truncated = bool(res.truncated)
        items = [i for i in res.by_class.get(cls["id"], []) if (i.proven or p["include_possible"]) and i.edge_rows]
        rows = self.gov.propagation.rows_for(items)
        matcher = self._matcher(p["target"], DATA_TARGET_TYPES, force_types=not p["target"].get("types"))
        allowed = set(p["allowed_labels"])
        exempt = self._carries([i.target_id for i in items], allowed)
        mask = p["masking_tag"]
        for it in sorted(items, key=lambda i: i.target_id):
            trow = rows.get(it.target_id)
            if trow is None or not matcher(trow):
                continue
            out.evaluated += 1
            if it.target_id in exempt:
                continue
            if mask and any(mask in row_tags(rows[n]) for n in it.nodes if n in rows):
                continue
            src = rows.get(it.source_id)
            path = [_asset_ev(rows[n]) for n in it.nodes if n in rows]
            out.findings.append(Finding("asset", trow["id"], trow["workspace_id"],
                                        f"'{trow['name']}' receives data labelled '{cls['name']}'" + ("" if it.proven else " (possible: lineage does not prove the column is used)"),
                                        {"classification": self.gov.classifications.ref_of(cls), "from_asset": _asset_ev(src) if src else None, "path": path, "confidence_floor": it.floor,
                                         "precision": it.precision, "proven": it.proven, "edges": [{"id": e["id"], "type": e["type"], "confidence": e["confidence"]} for e in it.edge_rows]},
                                        disc=cls["id"]))
        if out.truncated:
            out.notes.append("propagation hit a configured limit; exposures may be missing")
        return out

    def _rule_certified_sources_only(self, p: dict[str, Any]) -> Outcome:
        out = Outcome()
        cand = Candidates(self.db, self._matcher(p["selector"], p["target_types"], force_types=not p["selector"].get("types")), limit=min(self._limit(), 3000))
        targets = cand.all()
        out.truncated = cand.truncated
        src_types = set(p["source_types"])
        cert = self.gov.certification
        minc = str(self.setting("governance.propagation_min_confidence", "derived"))
        for t in targets:
            w = self.intel.graph.walk([t["id"]], direction="up", depth=p["depth"], min_confidence=minc, max_nodes=300)
            if w.truncated:
                out.notes.append(f"upstream walk of '{t['name']}' was cut off at 300 nodes")
            back: dict[str, builtins.list[tuple[str, dict[str, Any]]]] = {}
            for e in w.edges.values():
                f = M.flow(e["type"], e["src_id"], e["dst_id"])
                if f is not None:
                    back.setdefault(f[1], []).append((f[0], e))
            seen = {t["id"]}
            frontier = [t["id"]]
            found: dict[str, builtins.list[str]] = {}
            parent: dict[str, str] = {}
            while frontier:
                nxt: builtins.list[str] = []
                for n in frontier:
                    for up, _e in back.get(n, []):
                        if up in seen or up not in w.nodes:
                            continue
                        seen.add(up)
                        parent[up] = n
                        row = w.nodes[up]["row"]
                        if row["type"] in src_types and row["status"] == "active":
                            found[up] = []
                            continue
                        nxt.append(up)
                frontier = nxt
            out.evaluated += 1
            if not found:
                continue
            ids = sorted(found)
            states = cert.states("asset", ids)
            cert_ids = {i for i in ids if states.get(i) == "certified"}
            if p["include_products"]:
                prods = self.gov.products.products_of_assets([i for i in ids if i not in cert_ids])
                pstates = cert.states("product", sorted({x["id"] for ps in prods.values() for x in ps}))
                for aid, ps in prods.items():
                    if any(pstates.get(x["id"]) == "certified" for x in ps):
                        cert_ids.add(aid)
            bad = [i for i in ids if i not in cert_ids]
            if not bad:
                continue
            items = []
            for i in bad[:MAX_EVIDENCE_ITEMS]:
                chain, cur = [], i
                while cur != t["id"] and cur in parent:
                    chain.append(cur)
                    cur = parent[cur]
                items.append({**_asset_ev(w.nodes[i]["row"]), "asset_id": i, "certification": states.get(i, "none"), "path": [w.nodes[x]["row"]["name"] for x in reversed(chain)]})
            out.findings.append(Finding("asset", t["id"], t["workspace_id"], f"'{t['name']}' is built on {len(bad)} source(s) that are not certified",
                                        {"uncertified_sources": items, "uncertified_count": len(bad), "checked_sources": len(ids), "depth": p["depth"],
                                         "note": "a source counts as certified when it is certified or belongs to a certified data product; the search stops at the first source on each path"}))
        return out

    # ------------------------------------------------------------------------------------------------------------------------ reconciling violations
    def _reconcile(self, pol: dict[str, Any], outcome: Outcome) -> dict[str, int]:
        v = gov_violations.c
        now = now_iso()
        stats = {"opened": 0, "updated": 0, "resolved": 0, "reopened": 0}
        events: builtins.list[tuple[str, dict[str, Any], str | None]] = []
        with self.db.begin() as c:
            existing = {r["violation_key"]: r for r in fetch_all(c, select(gov_violations).where(v.policy_id == pol["id"]))}
            seen: set[str] = set()
            for f in outcome.findings:
                key = f.key
                seen.add(key)
                ex = existing.get(key)
                base = {"severity": pol["severity"], "message": f.message, "evidence": f.evidence, "workspace_id": f.workspace_id, "last_seen_at": now}
                payload = {"violation_id": ex["id"] if ex else None, "policy_id": pol["id"], "policy": pol["name"], "rule_type": pol["rule_type"], "subject_type": f.subject_type,
                           "subject_id": f.subject_id, "severity": pol["severity"], "message": f.message}
                if ex is None:
                    vid = new_id("vio")
                    c.execute(insert(gov_violations).values(id=vid, policy_id=pol["id"], violation_key=key, subject_type=f.subject_type, subject_id=f.subject_id, status="open",
                                                            first_seen_at=now, reopen_count=0, etag=new_etag(), **base))
                    stats["opened"] += 1
                    events.append(("governance.violation.opened", {**payload, "violation_id": vid}, f.workspace_id))
                    continue
                vals = dict(base)
                status = ex["status"]
                if status == "resolved":
                    vals.update(status="open", resolved_at=None, reopen_count=ex["reopen_count"] + 1, acknowledged_by=None, acknowledged_at=None, ack_comment=None, exception=None)
                    stats["reopened"] += 1
                    events.append(("governance.violation.opened", {**payload, "violation_id": ex["id"], "reopened": True}, f.workspace_id))
                elif status == "excepted" and self._exception_expired(ex["exception"]):
                    vals.update(status="open", exception=None)
                    stats["reopened"] += 1
                    events.append(("governance.violation.opened", {**payload, "violation_id": ex["id"], "reopened": True, "exception_expired": True}, f.workspace_id))
                else:
                    stats["updated"] += 1
                if vals.get("evidence") != ex["evidence"] or vals["message"] != ex["message"] or vals["severity"] != ex["severity"] or "status" in vals:
                    vals["etag"] = new_etag()
                c.execute(update(gov_violations).where(v.id == ex["id"]).values(**vals))
            if not outcome.truncated:
                for key, ex in existing.items():
                    if key in seen or ex["status"] == "resolved":
                        continue
                    c.execute(update(gov_violations).where(v.id == ex["id"]).values(status="resolved", resolved_at=now, etag=new_etag()))
                    stats["resolved"] += 1
                    events.append(("governance.violation.resolved", {"violation_id": ex["id"], "policy_id": pol["id"], "policy": pol["name"], "subject_type": ex["subject_type"],
                                                                      "subject_id": ex["subject_id"], "severity": ex["severity"]}, ex["workspace_id"]))
        for kind, payload, ws in events[:MAX_EVENTS_PER_RUN]:
            self.emit(kind, payload, workspace_id=ws, subject=payload["subject_id"])
        return stats

    @staticmethod
    def _exception_expired(exc: dict[str, Any] | None) -> bool:
        until = (exc or {}).get("until")
        if not until:
            return False
        try:
            return from_iso(str(until)) <= datetime.now(UTC)
        except Exception:  # noqa: BLE001
            return False

    def run(self, pol: dict[str, Any], principal: Principal | None) -> dict[str, Any]:
        """Evaluate one policy row now and store the result (no authorisation: callers check)."""
        started = time.monotonic()
        try:
            outcome = self._evaluate_rule(pol["rule_type"], pol["params"])
        except (BadRequest, NotFound) as exc:
            raise BadRequest(f"policy '{pol['name']}' cannot be evaluated: {exc.message}", remediation="edit the policy so that its parameters refer to existing objects") from exc
        stats = self._reconcile(pol, outcome)
        result = {"policy_id": pol["id"], "evaluated": outcome.evaluated, **stats, "unknown": outcome.unknown, "truncated": outcome.truncated, "notes": outcome.notes[:10],
                  "duration_ms": int((time.monotonic() - started) * 1000)}
        with self.db.begin() as c:
            c.execute(update(gov_policies).where(gov_policies.c.id == pol["id"]).values(last_evaluated_at=now_iso(), last_result={k: v for k, v in result.items() if k != "policy_id"}))
        self.audit(principal, "governance.policy_evaluated", resource_type="policy", resource_id=pol["id"], details={"name": pol["name"], **{k: result[k] for k in ("evaluated", "opened", "updated", "resolved", "reopened", "unknown", "truncated")}})
        self.emit("governance.policy.evaluated", {"policy_id": pol["id"], "policy": pol["name"], "rule_type": pol["rule_type"], **{k: result[k] for k in ("evaluated", "opened", "resolved", "reopened", "unknown", "truncated")}},
                  subject=pol["id"])
        return result

    def evaluate(self, principal: Principal, policy_id: str) -> dict[str, Any]:
        """``POST /policies/{id}/evaluate`` (``governance.approve`` org-wide). A disabled policy cannot be evaluated."""
        self.org_approver(principal)
        pol = self.row(policy_id)
        if not pol["enabled"]:
            raise Conflict(f"policy '{pol['name']}' is disabled", remediation="enable the policy first")
        return self.run(pol, principal)

    def evaluate_all(self, principal: Principal | None) -> builtins.list[dict[str, Any]]:
        """Evaluate every enabled policy (``principal`` ``None``: the post-scan processing). A broken policy reports its error instead of stopping the rest."""
        if principal is not None:
            self.org_approver(principal)
        with self.db.connect() as c:
            pols = fetch_all(c, select(gov_policies).where(gov_policies.c.enabled.is_(True), gov_policies.c.mode == "observe").order_by(gov_policies.c.id))
        results = []
        for pol in pols:
            try:
                results.append(self.run(pol, principal))
            except (BadRequest, Unavailable) as exc:
                results.append({"policy_id": pol["id"], "error": {"code": exc.code, "message": exc.message}})
        return results

    def preview(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """What a policy would report, without storing anything."""
        self.org_approver(principal)
        extra = set(body) - {"rule_type", "params", "severity", "limit"}
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}")
        rt = body.get("rule_type")
        if not isinstance(rt, str):
            raise BadRequest("rule_type is required", details={"supported": list(RULE_TYPES)})
        params = self.validate_params(rt, body.get("params"))
        severity = self._severity(body.get("severity"))
        limit = max(1, min(int(body.get("limit") or 100), 500))
        started = time.monotonic()
        outcome = self._evaluate_rule(rt, params)
        refs = self.visible_rows(principal, [f.subject_id for f in outcome.findings if f.subject_type == "asset"])
        items: builtins.list[dict[str, Any]] = []
        hidden = 0
        for f in outcome.findings:
            if f.subject_type == "asset" and f.subject_id not in refs:
                hidden += 1
                continue
            if len(items) < limit:
                items.append({"subject_type": f.subject_type, "subject_id": f.subject_id, "asset": compact_asset(refs[f.subject_id]) if f.subject_id in refs else None, "severity": severity,
                              "message": f.message, "evidence": self.redact(principal, f.evidence)})
        return {"rule_type": rt, "params": params, "evaluated": outcome.evaluated, "would_report": len(outcome.findings) - hidden, "unknown": outcome.unknown, "truncated": outcome.truncated,
                "notes": outcome.notes[:10], "items": items, "duration_ms": int((time.monotonic() - started) * 1000)}

    # ------------------------------------------------------------------------------------------------------------------------ violations
    def redact(self, principal: Principal, evidence: Any) -> Any:
        """Replace every ``{"asset_id": ...}`` object in ``evidence`` that names an asset the caller cannot see with ``{"hidden": true}``."""
        ids: set[str] = set()

        def collect(x: Any) -> None:
            if isinstance(x, dict):
                if isinstance(x.get("asset_id"), str):
                    ids.add(x["asset_id"])
                for y in x.values():
                    collect(y)
            elif isinstance(x, list):
                for y in x:
                    collect(y)

        collect(evidence)
        visible = set(self.visible_rows(principal, ids)) if ids else set()

        def walk(x: Any) -> Any:
            if isinstance(x, dict):
                if isinstance(x.get("asset_id"), str) and x["asset_id"] not in visible:
                    return {"hidden": True}
                return {k: walk(v) for k, v in x.items()}
            if isinstance(x, list):
                return [walk(y) for y in x]
            return x

        return walk(evidence)

    def _vpublic(self, principal: Principal, r: dict[str, Any], pols: dict[str, dict[str, Any]], arows: dict[str, dict[str, Any]], prods: dict[str, dict[str, Any]]) -> dict[str, Any]:
        pol = pols.get(r["policy_id"])
        return {"id": r["id"], "policy_id": r["policy_id"], "policy": {"id": r["policy_id"], "name": pol["name"], "rule_type": pol["rule_type"]} if pol else None,
                "subject_type": r["subject_type"], "asset": compact_asset(arows[r["subject_id"]]) if r["subject_type"] == "asset" and r["subject_id"] in arows else None,
                "product": prods.get(r["subject_id"]) if r["subject_type"] == "product" else None, "workspace_id": r["workspace_id"], "severity": r["severity"], "message": r["message"],
                "evidence": self.redact(principal, r["evidence"]), "status": r["status"], "first_seen_at": r["first_seen_at"], "last_seen_at": r["last_seen_at"], "resolved_at": r["resolved_at"],
                "reopen_count": r["reopen_count"], "acknowledged_by": r["acknowledged_by"], "acknowledged_at": r["acknowledged_at"], "ack_comment": r["ack_comment"], "exception": r["exception"], "etag": r["etag"]}

    def _vshape(self, principal: Principal, rows: builtins.list[dict[str, Any]]) -> builtins.list[dict[str, Any]]:
        pids = sorted({r["policy_id"] for r in rows})
        pols: dict[str, dict[str, Any]] = {}
        if pids:
            with self.db.connect() as c:
                for part in chunks(pids):
                    pols.update({p["id"]: p for p in fetch_all(c, select(gov_policies.c.id, gov_policies.c.name, gov_policies.c.rule_type).where(gov_policies.c.id.in_(part)))})
        arows = self.visible_rows(principal, [r["subject_id"] for r in rows if r["subject_type"] == "asset"])
        prods = self.gov.products.refs([r["subject_id"] for r in rows if r["subject_type"] == "product"])
        return [self._vpublic(principal, r, pols, arows, prods) for r in rows]

    def _visible_violations(self, principal: Principal, stmt: Any) -> Any:
        v = gov_violations.c
        return stmt.where(or_(v.subject_type == "product", v.subject_id.in_(self.scope_ids(principal))))

    def violations(self, principal: Principal, *, policy_id: str | None = None, asset_id: str | None = None, severity: str | None = None, status: str | None = None,
                   rule_type: str | None = None, workspace_id: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Violations of visible assets (and products), newest first. ``status`` defaults to ``open,acknowledged``; ``any`` returns all."""
        self.org_reader(principal)
        v = gov_violations.c
        stmt = self._visible_violations(principal, select(gov_violations))
        sts = (status or "open,acknowledged")
        if sts != "any":
            wanted = sts.split(",")
            if any(s not in VIOLATION_STATUSES for s in wanted):
                raise BadRequest(f"status must be 'any' or a comma list of {list(VIOLATION_STATUSES)}")
            stmt = stmt.where(v.status.in_(wanted))
        if policy_id:
            stmt = stmt.where(v.policy_id == policy_id)
        if asset_id:
            stmt = stmt.where(v.subject_id == self.asset(principal, asset_id)["id"])
        if severity:
            if severity not in SEVERITIES:
                raise BadRequest(f"severity must be one of {list(SEVERITIES)}")
            stmt = stmt.where(v.severity == severity)
        if workspace_id:
            stmt = stmt.where(v.workspace_id == workspace_id)
        if rule_type:
            stmt = stmt.where(v.policy_id.in_(select(gov_policies.c.id).where(gov_policies.c.rule_type == rule_type)))
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(v.id < cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(v.id.desc()).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return self._vshape(principal, rows[:limit]), nxt

    def _vrow(self, principal: Principal, vio_id: str) -> dict[str, Any]:
        self.org_reader(principal)
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_violations).where(gov_violations.c.id == vio_id))
        if r is None:
            raise NotFound(f"violation '{vio_id}' does not exist")
        if r["subject_type"] == "asset":
            self.asset(principal, r["subject_id"])
        return r

    def violation(self, principal: Principal, vio_id: str) -> dict[str, Any]:
        """One violation."""
        r = self._vrow(principal, vio_id)
        return self._vshape(principal, [r])[0]

    def _vwrite(self, principal: Principal, r: dict[str, Any], action: str, vals: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
        vals.update(etag=new_etag())
        with self.db.begin() as c:
            n = c.execute(update(gov_violations).where(gov_violations.c.id == r["id"], gov_violations.c.etag == r["etag"]).values(**vals)).rowcount
        if n == 0:
            raise_precondition(self._vrow(principal, r["id"])["etag"])
        self.audit(principal, "governance.violation_updated", resource_type="violation", resource_id=r["id"], workspace_id=r["workspace_id"], details={"action": action, "subject_id": r["subject_id"], **details})
        return self.violation(principal, r["id"])

    def _vcurate(self, principal: Principal, r: dict[str, Any]) -> None:
        principal.require("governance.curate", workspace_id=r["workspace_id"])

    def acknowledge(self, principal: Principal, vio_id: str, comment: Any = None) -> dict[str, Any]:
        """Mark an open violation as seen. It stays reported (``acknowledged``) until the finding disappears."""
        r = self._vrow(principal, vio_id)
        self._vcurate(principal, r)
        if r["status"] == "acknowledged":
            return self.violation(principal, vio_id)
        if r["status"] != "open":
            raise Conflict(f"only an open violation can be acknowledged (this one is {r['status']})")
        text = clean_text(comment, "comment", max_len=2000)
        return self._vwrite(principal, r, "acknowledge", {"status": "acknowledged", "acknowledged_by": principal.id, "acknowledged_at": now_iso(), "ack_comment": text}, {"comment": text})

    def reopen(self, principal: Principal, vio_id: str) -> dict[str, Any]:
        """``acknowledged`` / ``excepted`` back to ``open``."""
        r = self._vrow(principal, vio_id)
        self._vcurate(principal, r)
        if r["status"] == "open":
            return self.violation(principal, vio_id)
        if r["status"] not in ("acknowledged", "excepted"):
            raise Conflict(f"a {r['status']} violation cannot be reopened; it is reopened by the next evaluation if the finding returns")
        return self._vwrite(principal, r, "reopen", {"status": "open", "acknowledged_by": None, "acknowledged_at": None, "ack_comment": None, "exception": None}, {"previous": r["status"]})

    def set_exception(self, vio_id: str, *, reason: str, until: str | None, change_id: str, by: Principal | None) -> dict[str, Any]:
        """Grant an exception (called by an approved ``policy_exception`` change request). Idempotent for the same change."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_violations).where(gov_violations.c.id == vio_id))
        if r is None:
            raise NotFound(f"violation '{vio_id}' does not exist")
        if r["status"] == "excepted" and (r["exception"] or {}).get("change_id") == change_id:
            return r
        if r["status"] == "resolved":
            raise Conflict("the violation is resolved; there is nothing to except")
        exc = {"until": until, "change_id": change_id, "reason": reason, "approved_by": by.id if by else None}
        with self.db.begin() as c:
            c.execute(update(gov_violations).where(gov_violations.c.id == vio_id).values(status="excepted", exception=exc, etag=new_etag()))
        self.audit(by, "governance.violation_updated", resource_type="violation", resource_id=vio_id, workspace_id=r["workspace_id"], details={"action": "exception", "change_id": change_id, "until": until})
        return {**r, "status": "excepted", "exception": exc}

    # ------------------------------------------------------------------------------------------------------------------------ small helpers for other managers
    def counts_for(self, asset_ids: builtins.list[str]) -> dict[str, dict[str, Any]]:
        """``{asset_id: {"open": n, "by_severity": {...}, "points": ...}}`` for assets with open / acknowledged violations."""
        out: dict[str, dict[str, Any]] = {}
        v = gov_violations.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(asset_ids))):
                for sid, sev, n in c.execute(select(v.subject_id, v.severity, func.count()).where(v.subject_type == "asset", v.subject_id.in_(part), v.status.in_(("open", "acknowledged")))
                                             .group_by(v.subject_id, v.severity)):
                    d = out.setdefault(sid, {"open": 0, "by_severity": {}})
                    d["open"] += int(n)
                    d["by_severity"][sev] = int(n)
        return out

    def list_for_asset(self, principal: Principal, asset_id: str, limit: int = 50) -> builtins.list[dict[str, Any]]:
        """Open / acknowledged violations of one visible asset."""
        items, _ = self.violations(principal, asset_id=asset_id, limit=limit)
        return items

    def summary(self, principal: Principal, workspace_id: str | None = None) -> dict[str, Any]:
        """Counts of violations the caller can see."""
        v = gov_violations.c
        base = self._visible_violations(principal, select(v.status, v.severity, func.count()))
        if workspace_id:
            base = base.where(v.workspace_id == workspace_id)
        by_status: dict[str, int] = {}
        by_sev: dict[str, int] = {}
        with self.db.connect() as c:
            for st, sev, n in c.execute(base.group_by(v.status, v.severity)):
                by_status[st] = by_status.get(st, 0) + int(n)
                if st in ("open", "acknowledged"):
                    by_sev[sev] = by_sev.get(sev, 0) + int(n)
        return {"open": by_status.get("open", 0) + by_status.get("acknowledged", 0), "by_severity": by_sev, "by_status": by_status}

    def purge(self) -> int:
        """Delete resolved violations older than ``governance.violation_retention_days``."""
        days = int(self.setting("governance.violation_retention_days", 365))
        cutoff = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self.db.begin() as c:
            return int(c.execute(delete(gov_violations).where(gov_violations.c.status == "resolved", gov_violations.c.resolved_at < cutoff)).rowcount or 0)

    def merge_subject(self, from_id: str, to_id: str) -> int:
        """Drop the violations of a merged-away asset: their keys are derived from its id, so the next evaluation re-creates the findings that still hold under the survivor."""
        v = gov_violations.c
        with self.db.begin() as c:
            return int(c.execute(delete(gov_violations).where(v.subject_type == "asset", v.subject_id == from_id)).rowcount or 0)


__all__ = ["RULE_TYPES", "Finding", "Outcome", "PolicyManager"]
