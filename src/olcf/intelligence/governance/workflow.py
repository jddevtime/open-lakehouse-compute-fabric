# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Certification and the change-request workflow.

**Certification** has four states for an asset, a data product or a glossary term: ``none`` (the default; no row), ``draft``, ``certified``, ``deprecated``. A curator may move a subject
between ``none`` and ``draft``; ``certified`` and ``deprecated`` need ``governance.approve``. Certifying an asset records a *schema fingerprint* (a hash of its column names and
types) and the time. The certification is **dropped to** ``draft`` automatically - with an audit record, the event ``governance.certification.changed`` and ``invalidated_reason`` -
when, after it was given, the asset's schema changes (the fingerprint differs, or the version history shows a column added / removed / retyped since). A data product's
fingerprint is its member set. The check runs after scans, on ``dataset.schema_changed`` and periodically (:meth:`CertificationManager.invalidate_changed`); an asset without a known
schema cannot be checked and keeps its certification (the response says so with ``schema_known: false``).

**Change requests** (``chg_``) ask for something a curator may not do alone. Types: ``certify``, ``deprecate``, ``classification_change``, ``term_link``, ``ownership_change`` and
``policy_exception``. A request has a requester, a payload, a status (``pending`` -> ``approved`` | ``rejected`` | ``withdrawn``), optional reviewers (who is asked; any approver may decide),
and a comment trail. Approving applies the change in the same call; **applying is idempotent** (a repeat finds the work done) and is audited. The requester may not approve their own
request unless the setting ``governance.allow_self_approval`` is on.
"""

from __future__ import annotations

import builtins
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import String, and_, cast, func, insert, or_, select, update

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, new_etag
from olcf.intelligence.governance.common import CERT_STATES, Base, chunks, clean_text, compact_asset
from olcf.intelligence.governance.glossary import raise_precondition
from olcf.intelligence.governance.schema import (
    gov_certifications,
    gov_changes,
    gov_products,
    gov_terms,
    gov_violations,
)
from olcf.intelligence.schema import asset_versions
from olcf.security.rbac import Principal

CHANGE_TYPES = ("certify", "deprecate", "classification_change", "term_link", "ownership_change", "policy_exception")
CHANGE_SUBJECTS: dict[str, tuple[str, ...]] = {
    "certify": ("asset", "product", "term"), "deprecate": ("asset", "product", "term"), "classification_change": ("asset",), "term_link": ("asset", "product"),
    "ownership_change": ("asset", "domain", "product", "term"), "policy_exception": ("violation",),
}
CERT_SUBJECTS = ("asset", "product", "term")
NOT_CERTIFIABLE_ASSET_STATUS = ("removed", "placeholder")
MAX_COMMENTS = 200


def schema_fingerprint(attrs: dict[str, Any], child_columns: builtins.list[tuple[str, str]] | None = None) -> str | None:
    """Hash of an asset's column names and types, or ``None`` when no schema is known."""
    cols: dict[str, str] = {}
    for c in (attrs or {}).get("columns") or []:
        if isinstance(c, dict) and c.get("name") is not None:
            cols[str(c["name"]).lower()] = str(c.get("type"))
        elif isinstance(c, str):
            cols[c.lower()] = ""
    if not cols and child_columns:
        cols = {n.lower(): t for n, t in child_columns}
    if not cols:
        return None
    return hashlib.sha256(json.dumps(sorted(cols.items()), separators=(",", ":")).encode()).hexdigest()


class CertificationManager(Base):
    """Certification states and their automatic invalidation (see the module docstring)."""

    def states(self, subject_type: str, ids: builtins.list[str]) -> dict[str, str]:
        """``{subject id: state}`` for the subjects that have one other than ``none``."""
        out: dict[str, str] = {}
        t = gov_certifications.c
        with self.db.connect() as c:
            for part in chunks(list(dict.fromkeys(ids))):
                for r in c.execute(select(t.subject_id, t.state).where(t.subject_type == subject_type, t.subject_id.in_(part))):
                    out[r[0]] = r[1]
        return out

    def row(self, subject_type: str, subject_id: str) -> dict[str, Any] | None:
        """The certification row, if any."""
        with self.db.connect() as c:
            return fetch_one(c, select(gov_certifications).where(gov_certifications.c.subject_type == subject_type, gov_certifications.c.subject_id == subject_id))

    # ------------------------------------------------------------------------------------------------------------------------ fingerprints
    def fingerprint(self, subject_type: str, subject_id: str) -> str | None:
        """The current fingerprint of a subject (assets: schema; products: member set; terms: none)."""
        if subject_type == "asset":
            row = self.intel.graph.asset_rows([subject_id]).get(subject_id)
            if row is None:
                return None
            kids: builtins.list[tuple[str, str]] = []
            if not (row["attrs"] or {}).get("columns"):
                from olcf.intelligence.schema import assets

                with self.db.connect() as c:
                    kids = [(r["name"], str((r["attrs"] or {}).get("type"))) for r in fetch_all(c, select(assets).where(assets.c.parent_id == subject_id, assets.c.type == "column", assets.c.status != "removed"))]
            return schema_fingerprint(row["attrs"] or {}, kids)
        if subject_type == "product":
            ids = self.gov.products.member_ids(subject_id)
            return hashlib.sha256(json.dumps(ids).encode()).hexdigest()
        return None

    def _schema_changed_since(self, asset_id: str, since: str | None) -> bool:
        if not since:
            return False
        t = asset_versions.c
        with self.db.connect() as c:
            for r in c.execute(select(t.changes).where(t.asset_id == asset_id, t.created_at > since).order_by(t.id).limit(200)):
                if any(str(k).startswith("columns_") for k in (r[0] or {})):
                    return True
        return False

    def drift(self, row: dict[str, Any]) -> str | None:
        """Why a certified subject's certification no longer holds (``schema_changed`` / ``members_changed``), or ``None``."""
        if row["state"] != "certified":
            return None
        cur = self.fingerprint(row["subject_type"], row["subject_id"])
        if row["subject_type"] == "asset":
            if row["schema_fingerprint"] and cur and cur != row["schema_fingerprint"]:
                return "schema_changed"
            if self._schema_changed_since(row["subject_id"], row["certified_at"]):
                return "schema_changed"
        elif row["subject_type"] == "product" and row["schema_fingerprint"] and cur != row["schema_fingerprint"]:
            return "members_changed"
        return None

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def _public(self, subject_type: str, subject_id: str, row: dict[str, Any] | None) -> dict[str, Any]:
        base = {"subject_type": subject_type, "subject_id": subject_id}
        if row is None:
            return {**base, "state": "none", "certified_by": None, "certified_at": None, "comment": None, "invalidated_at": None, "invalidated_reason": None, "schema_known": None,
                    "schema_changed_since": False, "etag": None}
        drift = self.drift(row)
        known = None
        if subject_type == "asset":
            known = bool(row["schema_fingerprint"]) if row["state"] == "certified" else None
        return {**base, "state": row["state"], "certified_by": row["certified_by"], "certified_at": row["certified_at"], "comment": row["comment"], "invalidated_at": row["invalidated_at"],
                "invalidated_reason": row["invalidated_reason"], "schema_known": known, "schema_changed_since": drift is not None, "etag": row["etag"]}

    def _check_subject(self, principal: Principal, subject_type: str, subject_id: str, *, write: bool) -> tuple[str | None, str, str]:
        """Existence / visibility; returns ``(workspace_id, canonical id, name)``."""
        if subject_type not in CERT_SUBJECTS:
            raise BadRequest(f"certification applies to {list(CERT_SUBJECTS)}")
        if subject_type == "asset":
            r = self.asset(principal, subject_id)
            return r["workspace_id"], r["id"], r["name"]
        self.org_reader(principal)
        table = gov_products if subject_type == "product" else gov_terms
        with self.db.connect() as c:
            r2 = fetch_one(c, select(table.c.id, table.c.name).where(table.c.id == subject_id))
        if r2 is None:
            raise NotFound(f"{'data product' if subject_type == 'product' else 'glossary term'} '{subject_id}' does not exist")
        return None, r2["id"], r2["name"]

    def get(self, principal: Principal, subject_type: str, subject_id: str) -> dict[str, Any]:
        """The certification of a subject."""
        _, sid, _ = self._check_subject(principal, subject_type, subject_id, write=False)
        return self._public(subject_type, sid, self.row(subject_type, sid))

    def list(self, principal: Principal, *, state: str | None = None, subject_type: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Certified / draft / deprecated subjects the caller can see (``state`` defaults to ``certified,deprecated``)."""
        self.intel.scope(principal)
        wanted = (state or "certified,deprecated").split(",")
        if any(s not in CERT_STATES or s == "none" for s in wanted):
            raise BadRequest("state must be a comma list of draft, certified, deprecated")
        t = gov_certifications.c
        stmt = select(gov_certifications).where(t.state.in_(wanted))
        if subject_type:
            if subject_type not in CERT_SUBJECTS:
                raise BadRequest(f"subject_type must be one of {list(CERT_SUBJECTS)}")
            stmt = stmt.where(t.subject_type == subject_type)
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(or_(t.updated_at < cur[0], and_(t.updated_at == cur[0], t.subject_id > cur[1])))
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.updated_at.desc(), t.subject_id).limit(limit * 3 + 1))
        out: builtins.list[dict[str, Any]] = []
        arows = self.visible_rows(principal, [r["subject_id"] for r in rows if r["subject_type"] == "asset"])
        products = self.gov.products.refs([r["subject_id"] for r in rows if r["subject_type"] == "product"])
        terms = self.gov.glossary.refs([r["subject_id"] for r in rows if r["subject_type"] == "term"])
        last = None
        for r in rows:
            if len(out) >= limit:
                break
            last = r
            if r["subject_type"] == "asset":
                if r["subject_id"] not in arows:
                    continue
                subj = compact_asset(arows[r["subject_id"]])
            elif r["subject_type"] == "product":
                subj = products.get(r["subject_id"])  # type: ignore[assignment]
            else:
                subj = terms.get(r["subject_id"])  # type: ignore[assignment]
            if subj is None:
                continue
            out.append({**self._public(r["subject_type"], r["subject_id"], r), "subject": subj, "updated_at": r["updated_at"]})
        more = len(rows) > limit * 3 or (last is not None and len(out) >= limit and rows and rows[-1] is not last)
        nxt = encode_cursor([last["updated_at"], last["subject_id"]]) if more and last else None
        return out, nxt

    # ------------------------------------------------------------------------------------------------------------------------ writes
    def put(self, principal: Principal, subject_type: str, subject_id: str, state: Any, comment: str | None = None, if_match: str | None = None) -> dict[str, Any]:
        """``PUT .../certification``: move a subject to ``state`` (see the module docstring for who may)."""
        if state not in CERT_STATES:
            raise BadRequest(f"state must be one of {list(CERT_STATES)}")
        ws, sid, name = self._check_subject(principal, subject_type, subject_id, write=True)
        if state in ("certified", "deprecated"):
            principal.require("governance.approve", workspace_id=ws)
        else:
            principal.require("governance.curate", workspace_id=ws)
        cur = self.row(subject_type, sid)
        self.check_etag(if_match, cur["etag"] if cur else "")
        return self.set_state(subject_type, sid, state, by=principal, comment=clean_text(comment, "comment", max_len=2000), workspace_id=ws, name=name, expected_etag=cur["etag"] if cur else None)

    def set_state(self, subject_type: str, subject_id: str, state: str, *, by: Principal | None, comment: str | None = None, reason: str | None = None,
                  workspace_id: str | None = None, name: str | None = None, expected_etag: str | None = None, via_change: str | None = None) -> dict[str, Any]:
        """Set the state without authorisation checks (the caller has proven the rights). Idempotent: setting the current state again is a no-op."""
        if state == "certified":
            self._require_certifiable(subject_type, subject_id)
        cur = self.row(subject_type, subject_id)
        previous = cur["state"] if cur else "none"
        if previous == state and state != "certified":
            return self._public(subject_type, subject_id, cur)
        now = now_iso()
        fp = self.fingerprint(subject_type, subject_id) if state == "certified" else None
        vals = {"state": state, "comment": comment, "updated_by": by.id if by else None, "updated_at": now, "etag": new_etag()}
        if state == "certified":
            vals.update(certified_by=by.id if by else None, certified_at=now, schema_fingerprint=fp, invalidated_at=None, invalidated_reason=None)
        elif reason:
            vals.update(invalidated_at=now, invalidated_reason=reason)
        t = gov_certifications.c
        with self.db.begin() as c:
            if cur is None:
                c.execute(insert(gov_certifications).values(subject_type=subject_type, subject_id=subject_id, **vals))
            else:
                n = c.execute(update(gov_certifications).where(t.subject_type == subject_type, t.subject_id == subject_id, *( [t.etag == expected_etag] if expected_etag else [])).values(**vals)).rowcount
                if n == 0:
                    raise_precondition(self.row(subject_type, subject_id)["etag"])  # type: ignore[index]
        row = self.row(subject_type, subject_id)
        self.audit(by, "governance.certification_invalidated" if reason else "governance.certification_changed", resource_type=subject_type, resource_id=subject_id, workspace_id=workspace_id,
                   details={"state": state, "previous": previous, "name": name, "comment": comment, "reason": reason, "change_id": via_change})
        self.emit("governance.certification.changed", {"subject_type": subject_type, "subject_id": subject_id, "name": name, "state": state, "previous": previous, "reason": reason,
                                                       "by": by.name if by else None}, workspace_id=workspace_id, subject=subject_id)
        return self._public(subject_type, subject_id, row)

    def _require_certifiable(self, subject_type: str, subject_id: str) -> None:
        if subject_type == "asset":
            row = self.intel.graph.asset_rows([subject_id]).get(subject_id)
            if row is None:
                raise NotFound(f"asset '{subject_id}' does not exist")
            if row["status"] in NOT_CERTIFIABLE_ASSET_STATUS:
                raise BadRequest(f"a {row['status']} asset cannot be certified", remediation="wait for a scan to find the asset" if row["status"] == "placeholder" else None)
        elif subject_type == "term":
            with self.db.connect() as c:
                tr = fetch_one(c, select(gov_terms.c.status, gov_terms.c.name).where(gov_terms.c.id == subject_id))
            if tr is None:
                raise NotFound(f"glossary term '{subject_id}' does not exist")
            if tr["status"] != "approved":
                raise BadRequest(f"term '{tr['name']}' is {tr['status']}; only an approved term can be certified", remediation="approve the term first")

    # ------------------------------------------------------------------------------------------------------------------------ invalidation sweep
    def invalidate_changed(self, *, asset_ids: builtins.list[str] | None = None) -> builtins.list[dict[str, Any]]:
        """Drop to ``draft`` every certified subject whose schema / members changed since certification. Returns what was invalidated."""
        t = gov_certifications.c
        stmt = select(gov_certifications).where(t.state == "certified")
        if asset_ids is not None:
            stmt = stmt.where(t.subject_type == "asset", t.subject_id.in_(asset_ids))
        done: builtins.list[dict[str, Any]] = []
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.limit(50_000))
        for r in rows:
            why = self.drift(r)
            if why is None:
                continue
            ws, name = None, None
            if r["subject_type"] == "asset":
                a = self.intel.graph.asset_rows([r["subject_id"]]).get(r["subject_id"])
                ws, name = (a["workspace_id"], a["name"]) if a else (None, None)
            reason = "the schema changed after the asset was certified" if why == "schema_changed" else "the member set changed after the product was certified"
            self.set_state(r["subject_type"], r["subject_id"], "draft", by=None, comment=None, reason=reason, workspace_id=ws, name=name)
            done.append({"subject_type": r["subject_type"], "subject_id": r["subject_id"], "reason": why})
        return done


class WorkflowManager(Base):
    """Change requests (see the module docstring)."""

    # ------------------------------------------------------------------------------------------------------------------------ helpers
    def _subject_info(self, subject_type: str, subject_id: str) -> dict[str, Any] | None:
        """Existence and workspace of a subject without any caller context; ``None`` if gone."""
        if subject_type == "asset":
            r = self.intel.graph.asset_rows([subject_id]).get(subject_id)
            return {"workspace_id": r["workspace_id"], "name": r["name"], "ref": compact_asset(r), "row": r} if r else None
        if subject_type == "violation":
            with self.db.connect() as c:
                v = fetch_one(c, select(gov_violations).where(gov_violations.c.id == subject_id))
            return {"workspace_id": v["workspace_id"], "name": v["message"][:80], "ref": {"id": v["id"], "message": v["message"], "severity": v["severity"], "status": v["status"]}} if v else None
        if subject_type == "product":
            p = self.gov.products.row_or_none(subject_id)
            return {"workspace_id": None, "name": p["name"], "ref": {"id": p["id"], "name": p["name"], "status": p["status"]}} if p else None
        if subject_type == "term":
            with self.db.connect() as c:
                tr = fetch_one(c, select(gov_terms).where(gov_terms.c.id == subject_id))
            return {"workspace_id": None, "name": tr["name"], "ref": {"id": tr["id"], "name": tr["name"], "status": tr["status"]}} if tr else None
        if subject_type == "domain":
            d = self.gov.domains.row_or_none(subject_id)
            return {"workspace_id": None, "name": d["name"], "ref": {"id": d["id"], "name": d["name"]}} if d else None
        return None

    def _visible_filter(self, principal: Principal, stmt: Any) -> Any:
        vis = self.intel.scope(principal)
        t = gov_changes.c
        if vis is None:
            return stmt
        return stmt.where(or_(t.requester_id == principal.id, t.workspace_id.is_(None), t.workspace_id.in_(sorted(vis))))

    def _can_see(self, principal: Principal, row: dict[str, Any]) -> bool:
        if row["requester_id"] == principal.id:
            return True
        vis = principal.visible_workspaces("intelligence.read")
        if vis is not None and not vis:
            return False
        return row["workspace_id"] is None or vis is None or row["workspace_id"] in vis

    def _public(self, row: dict[str, Any], principal: Principal | None = None) -> dict[str, Any]:
        info = self._subject_info(row["subject_type"], row["subject_id"])
        subject = info["ref"] if info else None
        if info and principal is not None and row["subject_type"] == "asset" and not self.intel.can_see(principal, info["workspace_id"]):
            subject = {"hidden": True}
        return {"id": row["id"], "type": row["type"], "subject_type": row["subject_type"], "subject_id": row["subject_id"], "subject": subject, "workspace_id": row["workspace_id"],
                "status": row["status"], "requester": {"id": row["requester_id"], "name": row["requester_name"]}, "payload": row["payload"] or {}, "reviewers": row["reviewers"] or [],
                "comments": row["comments"] or [], "decided_by": ({"id": row["decided_by"], "name": row["decided_name"]} if row["decided_by"] else None), "decided_at": row["decided_at"],
                "decision_comment": row["decision_comment"], "applied_at": row["applied_at"], "apply_result": row["apply_result"] or {}, "etag": row["etag"],
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def row(self, change_id: str) -> dict[str, Any]:
        """The raw row of a change request (``NotFound`` if absent)."""
        with self.db.connect() as c:
            r = fetch_one(c, select(gov_changes).where(gov_changes.c.id == change_id))
        if r is None:
            raise NotFound(f"change request '{change_id}' does not exist")
        return r

    def _visible_row(self, principal: Principal, change_id: str) -> dict[str, Any]:
        r = self.row(change_id)
        self.intel.scope(principal)
        if not self._can_see(principal, r):
            raise NotFound(f"change request '{change_id}' does not exist")
        return r

    # ------------------------------------------------------------------------------------------------------------------------ validation
    def _validate(self, ctype: str, subject_type: str, subject_id: str, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise BadRequest("payload must be an object")
        if ctype == "certify":
            state = payload.get("state", "certified")
            if state != "certified":
                raise BadRequest("a certify request asks for state 'certified'; use a deprecate request to deprecate")
            return {"state": "certified"}
        if ctype == "deprecate":
            return {"reason": clean_text(payload.get("reason"), "reason", max_len=2000)} if payload.get("reason") else {}
        if ctype == "classification_change":
            add, remove = payload.get("add") or [], payload.get("remove") or []
            if not isinstance(add, list) or not isinstance(remove, list) or not (add or remove):
                raise BadRequest("classification_change needs 'add' and/or 'remove': lists of classification ids or names")
            out: dict[str, Any] = {"add": [], "remove": []}
            for key, lst in (("add", add), ("remove", remove)):
                for ref in lst:
                    out[key].append(self.gov.classifications.resolve(ref)["id"])
            return out
        if ctype == "term_link":
            action = payload.get("action", "link")
            if action not in ("link", "unlink"):
                raise BadRequest("action must be link or unlink")
            term_id = payload.get("term_id")
            if not isinstance(term_id, str):
                raise BadRequest("term_id is required")
            self.gov.glossary.row(term_id)
            return {"term_id": term_id, "action": action}
        if ctype == "ownership_change":
            add, remove = payload.get("add") or [], payload.get("remove") or []
            if not isinstance(add, list) or not isinstance(remove, list) or not (add or remove):
                raise BadRequest("ownership_change needs 'add' and/or 'remove'")
            for spec in add:
                self.gov.owners.parse(spec)
            for sel in remove:
                if not isinstance(sel, int):
                    self.gov.owners.parse(sel)
            return {"add": add, "remove": remove}
        if ctype == "policy_exception":
            reason = payload.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                raise BadRequest("policy_exception needs a 'reason'")
            out2: dict[str, Any] = {"reason": clean_text(reason, "reason", max_len=2000)}
            if payload.get("expires_at"):
                try:
                    exp = from_iso(str(payload["expires_at"]))
                except Exception:  # noqa: BLE001
                    raise BadRequest("expires_at must be an ISO-8601 time") from None
                if exp <= datetime.now(UTC):
                    raise BadRequest("expires_at must be in the future")
                out2["expires_at"] = str(payload["expires_at"])
            return out2
        raise BadRequest(f"unknown change type '{ctype}'", details={"supported": list(CHANGE_TYPES)})

    def _resolve_reviewers(self, reviewers: Any) -> builtins.list[dict[str, Any]]:
        if reviewers in (None, []):
            return []
        if not isinstance(reviewers, list) or len(reviewers) > 20:
            raise BadRequest("reviewers must be a list of at most 20 users")
        out = []
        for ref in reviewers:
            u = self.gov.owners.find_user(str(ref))
            if u is None:
                raise BadRequest(f"no active OLCF user matches reviewer '{ref}'")
            out.append({"id": u["id"], "name": u["display_name"] or u["name"]})
        return out

    # ------------------------------------------------------------------------------------------------------------------------ create / read
    def create(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """Open a change request (``governance.curate`` in the workspace of the subject; org-wide for org-level subjects)."""
        extra = set(body) - {"type", "subject_type", "subject_id", "payload", "reviewers", "comment"}
        if extra:
            raise BadRequest(f"unknown field(s) {sorted(extra)}")
        ctype, stype, sid = body.get("type"), body.get("subject_type"), body.get("subject_id")
        if ctype not in CHANGE_TYPES:
            raise BadRequest(f"type must be one of {list(CHANGE_TYPES)}")
        if stype not in CHANGE_SUBJECTS[ctype]:
            raise BadRequest(f"a {ctype} request applies to {list(CHANGE_SUBJECTS[ctype])}, not '{stype}'")
        if not isinstance(sid, str) or not sid:
            raise BadRequest("subject_id is required")
        self.intel.scope(principal)
        info = self._subject_info(stype, sid)
        if stype == "asset":
            arow = self.asset(principal, sid)
            sid, info = arow["id"], self._subject_info("asset", arow["id"])
        if info is None:
            raise NotFound(f"{stype} '{sid}' does not exist")
        ws = info["workspace_id"]
        if stype == "violation":
            vrow = fetch_one_row(self.db, gov_violations, sid)
            if vrow and vrow["subject_type"] == "asset":
                self.asset(principal, vrow["subject_id"])
        principal.require("governance.curate", workspace_id=ws)
        payload = self._validate(ctype, stype, sid, body.get("payload") if body.get("payload") is not None else {})
        reviewers = self._resolve_reviewers(body.get("reviewers"))
        comment = clean_text(body.get("comment"), "comment", max_len=2000)
        now = now_iso()
        cid = new_id("chg")
        comments = [{"at": now, "by": principal.id, "by_name": principal.name, "text": comment or "", "action": "created"}]
        with self.db.begin() as c:
            if ctype in ("certify", "deprecate", "policy_exception", "classification_change", "term_link"):
                dup = fetch_one(c, select(gov_changes.c.id).where(gov_changes.c.type == ctype, gov_changes.c.subject_type == stype, gov_changes.c.subject_id == sid,
                                                                  gov_changes.c.status == "pending", cast(gov_changes.c.payload, String) == _canonical(payload)))
                if dup:
                    raise Conflict(f"an identical {ctype} request is already pending ({dup['id']})", details={"change_id": dup["id"]})
            c.execute(insert(gov_changes).values(id=cid, type=ctype, subject_type=stype, subject_id=sid, workspace_id=ws, requester_id=principal.id, requester_name=principal.name, payload=payload,
                                                 status="pending", reviewers=reviewers, comments=comments, apply_result={}, etag=new_etag(), created_at=now, updated_at=now))
        self.audit(principal, "governance.change_requested", resource_type="change", resource_id=cid, workspace_id=ws, details={"type": ctype, "subject_type": stype, "subject_id": sid})
        evt = {"change_id": cid, "type": ctype, "subject_type": stype, "subject_id": sid, "subject": info["name"], "requester": principal.name, "reviewers": [r["id"] for r in reviewers]}
        self.emit("governance.change.requested", evt, workspace_id=ws, subject=cid)
        self.emit("governance.review.requested", {"kind": "change_request", "count": 1, **evt}, workspace_id=ws, subject=cid)
        return self._public(self.row(cid), principal)

    def get(self, principal: Principal, change_id: str) -> dict[str, Any]:
        """One change request with its comments."""
        return self._public(self._visible_row(principal, change_id), principal)

    def list(self, principal: Principal, *, status: str | None = None, type: str | None = None, subject_type: str | None = None, subject_id: str | None = None,
             requester: str | None = None, reviewer: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[builtins.list[dict[str, Any]], str | None]:
        """Change requests the caller can see, newest first."""
        t = gov_changes.c
        stmt = self._visible_filter(principal, select(gov_changes))
        if status:
            if any(s not in ("pending", "approved", "rejected", "withdrawn") for s in status.split(",")):
                raise BadRequest("status must be a comma list of pending, approved, rejected, withdrawn")
            stmt = stmt.where(t.status.in_(status.split(",")))
        if type:
            stmt = stmt.where(t.type == type)
        if subject_type:
            stmt = stmt.where(t.subject_type == subject_type)
        if subject_id:
            stmt = stmt.where(t.subject_id == subject_id)
        if requester:
            stmt = stmt.where(t.requester_id == (principal.id if requester == "me" else requester))
        if reviewer:
            rid = principal.id if reviewer == "me" else reviewer
            stmt = stmt.where(cast(t.reviewers, String).like(f'%"{rid}"%'))
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(t.id < cur[0])
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(t.id.desc()).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["id"]]) if len(rows) > limit else None
        return [self._public(r, principal) for r in rows[:limit]], nxt

    def pending_count(self, principal: Principal, workspace_id: str | None = None) -> int:
        """Pending change requests the caller can see (optionally in one workspace)."""
        t = gov_changes.c
        stmt = self._visible_filter(principal, select(func.count()).select_from(gov_changes).where(t.status == "pending"))
        if workspace_id:
            stmt = stmt.where(t.workspace_id == workspace_id)
        with self.db.connect() as c:
            return int(c.execute(stmt).scalar() or 0)

    def for_subject(self, principal: Principal, subject_type: str, subject_id: str, *, status: str = "pending") -> builtins.list[dict[str, Any]]:
        """Change requests about one subject (used by the per-asset view)."""
        items, _ = self.list(principal, status=status, subject_type=subject_type, subject_id=subject_id, limit=50)
        return items

    # ------------------------------------------------------------------------------------------------------------------------ decisions
    def _append(self, row: dict[str, Any], principal: Principal | None, text: str | None, action: str) -> builtins.list[dict[str, Any]]:
        comments = list(row["comments"] or [])[-(MAX_COMMENTS - 1):]
        comments.append({"at": now_iso(), "by": principal.id if principal else None, "by_name": principal.name if principal else "system", "text": text or "", "action": action})
        return comments

    def comment(self, principal: Principal, change_id: str, text: Any) -> dict[str, Any]:
        """Add a comment (``governance.curate``)."""
        row = self._visible_row(principal, change_id)
        principal.require("governance.curate", workspace_id=row["workspace_id"])
        if not isinstance(text, str) or not text.strip():
            raise BadRequest("text must be a non-empty string")
        text = clean_text(text, "text", max_len=4000) or ""
        with self.db.begin() as c:
            c.execute(update(gov_changes).where(gov_changes.c.id == row["id"]).values(comments=self._append(row, principal, text, "comment"), updated_at=now_iso(), etag=new_etag()))
        self.audit(principal, "governance.change_commented", resource_type="change", resource_id=row["id"], workspace_id=row["workspace_id"], details={"length": len(text)})
        return self.get(principal, change_id)

    def _decide(self, principal: Principal, change_id: str, status: str, comment: str | None) -> dict[str, Any]:
        row = self._visible_row(principal, change_id)
        principal.require("governance.approve", workspace_id=row["workspace_id"])
        if row["status"] != "pending":
            raise Conflict(f"change request '{change_id}' is already {row['status']}")
        if status == "approved" and row["requester_id"] == principal.id and not self.setting("governance.allow_self_approval", False):
            raise self.denied("you cannot approve your own change request", "ask another approver, or turn on governance.allow_self_approval (not recommended)")
        now = now_iso()
        with self.db.begin() as c:
            n = c.execute(update(gov_changes).where(gov_changes.c.id == row["id"], gov_changes.c.status == "pending").values(
                status=status, decided_by=principal.id, decided_name=principal.name, decided_at=now, decision_comment=comment, updated_at=now, etag=new_etag(),
                comments=self._append(row, principal, comment, "approve" if status == "approved" else "reject"))).rowcount
        if n == 0:
            raise Conflict(f"change request '{change_id}' was decided concurrently")
        self.audit(principal, "governance.change_decided", resource_type="change", resource_id=row["id"], workspace_id=row["workspace_id"],
                   details={"decision": status, "type": row["type"], "subject_type": row["subject_type"], "subject_id": row["subject_id"], "requester": row["requester_name"]})
        self.emit("governance.change.decided", {"change_id": row["id"], "type": row["type"], "decision": status, "subject_type": row["subject_type"], "subject_id": row["subject_id"],
                                                "requester": row["requester_name"], "decided_by": principal.name}, workspace_id=row["workspace_id"], subject=row["id"])
        if status == "approved":
            self._apply_row(self.row(change_id), principal)
        return self.get(principal, change_id)

    def approve(self, principal: Principal, change_id: str, comment: Any = None) -> dict[str, Any]:
        """Approve and apply a request (``governance.approve``; not your own unless ``governance.allow_self_approval``)."""
        return self._decide(principal, change_id, "approved", clean_text(comment, "comment", max_len=2000))

    def reject(self, principal: Principal, change_id: str, comment: Any) -> dict[str, Any]:
        """Reject a request; a comment is required."""
        text = clean_text(comment, "comment", max_len=2000)
        if not text or not text.strip():
            raise BadRequest("a rejection needs a comment that tells the requester why")
        return self._decide(principal, change_id, "rejected", text)

    def withdraw(self, principal: Principal, change_id: str) -> dict[str, Any]:
        """The requester takes a pending request back."""
        row = self._visible_row(principal, change_id)
        if row["requester_id"] != principal.id:
            raise self.denied("only the requester can withdraw a change request")
        if row["status"] != "pending":
            raise Conflict(f"change request '{change_id}' is already {row['status']}")
        now = now_iso()
        with self.db.begin() as c:
            c.execute(update(gov_changes).where(gov_changes.c.id == row["id"], gov_changes.c.status == "pending").values(
                status="withdrawn", decided_at=now, updated_at=now, etag=new_etag(), comments=self._append(row, principal, None, "withdraw")))
        self.audit(principal, "governance.change_withdrawn", resource_type="change", resource_id=row["id"], workspace_id=row["workspace_id"], details={"type": row["type"]})
        return self.get(principal, change_id)

    def apply(self, principal: Principal, change_id: str) -> dict[str, Any]:
        """Re-apply an approved change whose application failed (``governance.approve``); a change that is already applied is returned unchanged."""
        row = self._visible_row(principal, change_id)
        principal.require("governance.approve", workspace_id=row["workspace_id"])
        if row["status"] != "approved":
            raise Conflict(f"only an approved change request can be applied (this one is {row['status']})")
        self._apply_row(row, principal)
        return self.get(principal, change_id)

    # ------------------------------------------------------------------------------------------------------------------------ applying
    def _apply_row(self, row: dict[str, Any], actor: Principal) -> dict[str, Any]:
        if row["applied_at"]:
            return row["apply_result"] or {}
        try:
            result = self._do_apply(row, actor)
            result["ok"] = True
        except Exception as exc:  # noqa: BLE001 - the decision stands; the failure is recorded and can be retried
            result = {"ok": False, "error": {"code": getattr(exc, "code", "internal"), "message": getattr(exc, "message", str(exc))[:500]}}
        now = now_iso()
        with self.db.begin() as c:
            c.execute(update(gov_changes).where(gov_changes.c.id == row["id"]).values(applied_at=now if result["ok"] else None, apply_result=result, updated_at=now, etag=new_etag()))
        self.audit(actor, "governance.change_applied", resource_type="change", resource_id=row["id"], workspace_id=row["workspace_id"], outcome="success" if result["ok"] else "failure",
                   details={"type": row["type"], "result": result})
        return result

    def _do_apply(self, row: dict[str, Any], actor: Principal) -> dict[str, Any]:
        ctype, stype, sid, payload = row["type"], row["subject_type"], row["subject_id"], row["payload"] or {}
        info = self._subject_info(stype, sid)
        if info is None:
            raise NotFound(f"{stype} '{sid}' no longer exists")
        ws = info["workspace_id"]
        if ctype == "certify":
            out = self.gov.certification.set_state(stype, sid, "certified", by=actor, comment=f"approved change {row['id']}", workspace_id=ws, name=info["name"], via_change=row["id"])
            return {"certification": out["state"]}
        if ctype == "deprecate":
            out = self.gov.certification.set_state(stype, sid, "deprecated", by=actor, comment=payload.get("reason"), workspace_id=ws, name=info["name"], via_change=row["id"])
            with self.db.begin() as c:
                if stype == "product":
                    c.execute(update(gov_products).where(gov_products.c.id == sid).values(status="deprecated", updated_at=now_iso(), etag=new_etag()))
                elif stype == "term":
                    c.execute(update(gov_terms).where(gov_terms.c.id == sid).values(status="deprecated", updated_at=now_iso(), etag=new_etag()))
            return {"certification": out["state"]}
        if ctype == "classification_change":
            added, removed, already = [], [], []
            for cid in payload.get("add") or []:
                try:
                    self.gov.classifications.assign(actor, sid, cid, comment=f"approved change {row['id']}", authorized_row=info["row"], via_change=row["id"])
                    added.append(cid)
                except Conflict:
                    already.append(cid)
            for cid in payload.get("remove") or []:
                try:
                    self.gov.classifications.remove(actor, sid, cid, authorized_row=info["row"])
                    removed.append(cid)
                except NotFound:
                    already.append(cid)
            return {"added": added, "removed": removed, "already_done": already}
        if ctype == "term_link":
            if payload["action"] == "link":
                changed = self.gov.glossary.link_subject(actor, payload["term_id"], stype, sid, comment=f"approved change {row['id']}")
            else:
                changed = self.gov.glossary.unlink_subject(actor, payload["term_id"], stype, sid)
            return {"changed": changed, "action": payload["action"]}
        if ctype == "ownership_change":
            n_added, n_removed, n_already = 0, 0, 0
            for spec in payload.get("add") or []:
                try:
                    self.gov.owners.add(actor, stype, sid, spec, authorized=True)
                    n_added += 1
                except Conflict:
                    n_already += 1
            for sel in payload.get("remove") or []:
                n_removed += self.gov.owners.remove_matching(stype, sid, sel)
            return {"added": n_added, "removed": n_removed, "already_present": n_already}
        if ctype == "policy_exception":
            v = self.gov.policies.set_exception(sid, reason=payload["reason"], until=payload.get("expires_at"), change_id=row["id"], by=actor)
            return {"violation_status": v["status"], "until": payload.get("expires_at")}
        raise BadRequest(f"unknown change type '{ctype}'")


def _canonical(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=False)


def fetch_one_row(db: Any, table: Any, ident: str) -> dict[str, Any] | None:
    """Row of ``table`` by id."""
    with db.connect() as c:
        return fetch_one(c, select(table).where(table.c.id == ident))
