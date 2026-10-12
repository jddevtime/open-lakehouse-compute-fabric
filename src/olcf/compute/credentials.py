# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Worker credential lifecycle: machine-certificate renewal, certificate expiry tracking, worker-token max age and rotation (CONTRACTS 4.2 "Credential renewal").

A worker holds two credentials: the **worker token** that authenticates its control channel and the **machine certificate** (mTLS) issued from a CSR whose private key never
leaves the node. Both used to live forever / 90 days with no renewal. This module is the controller side of the answer:

* **Renewal over the channel.** The agent sends ``renew_cert`` (a CSR) on its authenticated connection when the hello acknowledgement says it is time (``credentials.renew_after``).
  The controller signs when the worker identity is valid and the certificate is inside the renewal window (``compute.worker_cert_renew_before_days``, default 30); the subject is
  always ``CN=<worker id>`` whatever the CSR asks for. The same exchange delivers a rotated worker token when a max age is configured
  (``compute.worker_token_max_age_days``, default 0 = tokens do not expire and are never rotated: existing installs behave as before).
* **Nothing is revoked before it is replaced.** The superseded token and certificate stay valid for a grace period (``compute.worker_token_grace_seconds``) and are revoked when
  the node first connects with the new ones - or when the grace period ends (leader sweep) - so a response lost in transit never locks a node out.
* **Operator command.** ``POST /v1/workers/{id}/renew`` grants a short window in which the next ``renew_cert`` is honoured whatever the dates say and asks the agent
  (``renew_credentials`` command) to run the exchange now.
* **Visibility.** ``cert_expires_at`` / ``token_expires_at`` on the worker record, the hello acknowledgement, the topology and the ``worker-certificates`` health check
  (warn below 30 days, fail below 7 days or expired) and the ``worker_certificate_expiry`` alert rule.

State lives in three ``workers`` columns (``cert_expires_at``, ``token_expires_at``, ``credentials`` JSON) that only this module writes after enrolment, so a heartbeat can never
overwrite a pending rotation.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from cryptography import x509
from sqlalchemy import update

from olcf.compute import schema as C
from olcf.core.errors import BadRequest, Conflict, Forbidden, OlcfError
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.timeutil import from_iso, iso_in, now_iso, to_iso, utcnow
from olcf.security.rbac import SYSTEM_PRINCIPAL, Principal

if TYPE_CHECKING:
    from olcf.compute.hub import WorkerHub
    from olcf.compute.registry import ComputeRegistry
    from olcf.core.context import AppContext

log = logging.getLogger("olcf.compute.credentials")

DAY = 86400.0
#: Health: a certificate with fewer days left than this is a warning / a failure (expired counts as failed).
WARN_DAYS = 30.0
FAIL_DAYS = 7.0
#: Scheduled renewals are refused when the previous one is younger than this (a misbehaving agent must not mint certificates in a loop). Operator requests are exempt.
MIN_RENEW_INTERVAL_SECONDS = 30.0
#: How long an operator's ``olcf worker renew`` lets the node's ``renew_cert`` through regardless of the dates.
GRANT_SECONDS = 120.0

DEFAULT_RENEW_BEFORE_DAYS = 30.0
DEFAULT_CERT_LIFETIME_DAYS = 90.0
DEFAULT_TOKEN_GRACE_SECONDS = 86400.0

SETTING_PREFIX = "compute.worker_"


class RenewalRefused(OlcfError):
    """A ``renew_cert`` the controller does not honour; ``code`` tells the agent why (``renewal_not_due``, ``renewal_too_soon``, ``bad_csr``, ``auto_renew_disabled``, ...)."""

    status = 409

    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None, remediation: str | None = None) -> None:
        super().__init__(message, details=details, remediation=remediation)
        self.code = code


@dataclass(frozen=True)
class Policy:
    """The effective credential policy (settings ``compute.worker_cert_*`` / ``compute.worker_token_*``)."""

    renew_before_days: float = DEFAULT_RENEW_BEFORE_DAYS
    cert_lifetime_days: float = DEFAULT_CERT_LIFETIME_DAYS
    auto_renew: bool = True
    token_max_age_days: float = 0.0
    grace_seconds: float = DEFAULT_TOKEN_GRACE_SECONDS

    @property
    def cert_window_days(self) -> float:
        """The renewal window, never wider than half the lifetime: a freshly issued certificate must not be due again at once."""
        return max(0.0, min(self.renew_before_days, self.cert_lifetime_days / 2.0))

    @property
    def token_window_days(self) -> float:
        return max(0.0, min(self.renew_before_days, self.token_max_age_days / 2.0)) if self.token_max_age_days > 0 else 0.0


def _num(value: Any, default: float, *, minimum: float = 0.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v >= minimum else default


def cert_not_after(cert_pem: str) -> str:
    return to_iso(x509.load_pem_x509_certificate(cert_pem.encode()).not_valid_after_utc)


def _doc(row: dict[str, Any]) -> dict[str, Any]:
    d = row.get("credentials")
    return dict(d) if isinstance(d, dict) else {}


def days_left(expires_at: str | None, now: datetime | None = None) -> float | None:
    if not expires_at:
        return None
    return (from_iso(expires_at) - (now or utcnow())).total_seconds() / DAY


def expiring_worker_certificates(registry: ComputeRegistry, within_days: float, now: datetime | None = None) -> list[dict[str, Any]]:
    """Non-revoked workers whose recorded certificate expires within ``within_days`` (already expired included), soonest first. Shared by the health check and the alert rule."""
    now = now or utcnow()
    out = []
    for w in registry.list_workers(include_revoked=False):
        left = days_left(w.get("cert_expires_at"), now)
        if left is not None and left <= within_days:
            out.append({"id": w["id"], "name": w["name"], "state": w["state"], "cert_expires_at": w["cert_expires_at"], "days_left": round(left, 2)})
    return sorted(out, key=lambda r: r["days_left"])


class WorkerCredentials:
    """Registered on the compute service as ``credentials``. See the module docstring."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: WorkerHub) -> None:
        self.ctx, self.registry, self.hub = ctx, registry, hub
        self._swept = 0.0

    # ------------------------------------------------------------------ policy
    def policy(self) -> Policy:
        s = self.registry.setting
        auto = s(SETTING_PREFIX + "cert_auto_renew", True)
        if isinstance(auto, str):
            auto = auto.strip().lower() not in ("false", "0", "no", "off")
        return Policy(
            renew_before_days=_num(s(SETTING_PREFIX + "cert_renew_before_days", DEFAULT_RENEW_BEFORE_DAYS), DEFAULT_RENEW_BEFORE_DAYS),
            cert_lifetime_days=_num(s(SETTING_PREFIX + "cert_lifetime_days", DEFAULT_CERT_LIFETIME_DAYS), DEFAULT_CERT_LIFETIME_DAYS, minimum=0.001),
            auto_renew=bool(auto),
            token_max_age_days=_num(s(SETTING_PREFIX + "token_max_age_days", 0.0), 0.0),
            grace_seconds=_num(s(SETTING_PREFIX + "token_grace_seconds", DEFAULT_TOKEN_GRACE_SECONDS), DEFAULT_TOKEN_GRACE_SECONDS))

    # ------------------------------------------------------------------ what is due
    @staticmethod
    def cert_due(row: dict[str, Any], pol: Policy, now: datetime) -> bool:
        if not row.get("cert_serial"):
            return False  # enrolled without a certificate: nothing to renew
        left = days_left(row.get("cert_expires_at"), now)
        return True if left is None else left <= pol.cert_window_days

    @staticmethod
    def token_due(row: dict[str, Any], pol: Policy, now: datetime) -> bool:
        if pol.token_max_age_days <= 0:
            return False  # default: worker tokens neither expire nor rotate
        left = days_left(row.get("token_expires_at"), now)
        return True if left is None else left <= pol.token_window_days  # a token without an expiry predates the policy: adopt it at the next exchange

    def renew_after(self, row: dict[str, Any], pol: Policy | None = None, now: datetime | None = None) -> str | None:
        """When the agent should next ask for renewal (ISO, may be in the past = now); ``None`` when nothing needs renewing or automatic renewal is off."""
        pol = pol or self.policy()
        now = now or utcnow()
        if not pol.auto_renew:
            return None
        cands: list[datetime] = []
        if row.get("cert_serial"):
            exp = row.get("cert_expires_at")
            cands.append(from_iso(exp) - timedelta(days=pol.cert_window_days) if exp else now)
        if pol.token_max_age_days > 0:
            exp = row.get("token_expires_at")
            cands.append(from_iso(exp) - timedelta(days=pol.token_window_days) if exp else now)
        return to_iso(min(cands)) if cands else None

    def hello_block(self, row: dict[str, Any]) -> dict[str, Any]:
        """The ``credentials`` object of the hello acknowledgement: tells the agent when to renew."""
        pol = self.policy()
        return {"cert_expires_at": row.get("cert_expires_at"), "token_expires_at": row.get("token_expires_at"), "renew_after": self.renew_after(row, pol),
                "auto_renew": pol.auto_renew, "renew_before_days": pol.cert_window_days, "token_max_age_days": pol.token_max_age_days}

    def describe(self, row: dict[str, Any]) -> dict[str, Any]:
        """Operator view of a worker's credentials (API ``credentials`` object): expiry, next renewal, pending rotation, last renewal. Never ids or key material."""
        d = _doc(row)
        pol = self.policy()
        rot = d.get("rotation") or {}
        left = days_left(row.get("cert_expires_at"))
        return {"cert_expires_at": row.get("cert_expires_at"), "cert_days_left": None if left is None else round(left, 2),
                "token_expires_at": row.get("token_expires_at"), "renew_after": self.renew_after(row, pol), "auto_renew": pol.auto_renew,
                "rotation_pending": bool(rot), "rotation_grace_until": rot.get("prev_until"), "last_renewal": d.get("last_renewal")}

    # ------------------------------------------------------------------ enrolment bookkeeping
    def enrolment_values(self, *, cert_pem: str | None, token_row: dict[str, Any]) -> dict[str, Any]:
        """Column values for a freshly enrolled worker (also clears the state of a replaced worker)."""
        return {"cert_expires_at": cert_not_after(cert_pem) if cert_pem else None, "token_expires_at": token_row.get("expires_at"), "credentials": {}}

    def token_ttl_seconds(self) -> int | None:
        pol = self.policy()
        return int(pol.token_max_age_days * DAY) if pol.token_max_age_days > 0 else None

    def cert_lifetime_days(self) -> float:
        return self.policy().cert_lifetime_days

    # ------------------------------------------------------------------ connection hooks (hub threads)
    def observe_hello(self, row: dict[str, Any], reported: Any) -> None:
        """A worker without a recorded certificate expiry (enrolled before tracking existed) learns it from the certificate its agent says it holds - but only when that is the
        certificate this controller issued (same serial)."""
        if not isinstance(reported, dict) or not row.get("cert_serial") or row.get("cert_expires_at"):
            return
        if str(reported.get("serial") or "").lower() != str(row["cert_serial"]).lower():
            return
        try:
            exp = to_iso(from_iso(str(reported["not_after"])))
        except (KeyError, ValueError):
            return
        self.registry.update_worker(row["id"], cert_expires_at=exp)
        row["cert_expires_at"] = exp

    def on_authenticated(self, row: dict[str, Any], token_id: str, presented_serial: str | None) -> None:
        """A connection authenticated. If it used the new token (and, when a client certificate was presented, the new certificate), the superseded ones are revoked now."""
        rot = _doc(row).get("rotation")
        if not rot or token_id != row.get("token_id"):
            return
        if rot.get("prev_serial") and presented_serial and presented_serial != row.get("cert_serial"):
            return  # connected with the new token but still the old certificate: the swap has not happened yet
        self.finalize(row, "new credentials in use")

    def finalize(self, row: dict[str, Any], outcome: str, actor: Principal | None = None) -> None:
        """Revoke the superseded token and certificate of a rotation and clear its state."""
        with self.registry.db_lock(f"worker-credentials-{row['id']}", ttl=60, wait=15):
            fresh = self.registry.get_worker(row["id"])
            doc = _doc(fresh)
            rot = doc.pop("rotation", None)
            if not rot:
                return
            revoked: dict[str, Any] = {}
            if rot.get("prev_token_id") and rot["prev_token_id"] != fresh.get("token_id"):
                revoked["token"] = bool(self.ctx.tokens.revoke(rot["prev_token_id"]))
            if rot.get("prev_serial") and rot["prev_serial"] != fresh.get("cert_serial"):
                self.hub.revoke_serial(rot["prev_serial"])
                revoked["certificate"] = True
            with self.ctx.db.begin() as conn:
                conn.execute(update(C.workers).where(C.workers.c.id == row["id"]).values(credentials=doc, updated_at=now_iso()))
        self.ctx.audit.record(actor or SYSTEM_PRINCIPAL, "worker.credentials_rotation_completed", resource_type="worker", resource_id=row["id"],
                              details={"name": row["name"], "outcome": outcome, "revoked": revoked})

    def sweep_tick(self) -> None:
        """Scheduler hook (leader): :meth:`sweep`, at most every 5 seconds."""
        now = time.monotonic()
        if now - self._swept < 5.0:
            return
        self._swept = now
        self.sweep()

    def sweep(self) -> int:
        """Leader hook: rotations whose grace period ended are closed (the superseded credentials are revoked). Returns how many."""
        n, now = 0, utcnow()
        for w in self.registry.list_workers(include_revoked=False):
            rot = _doc(w).get("rotation")
            if not rot or not rot.get("prev_until"):
                continue
            try:
                over = from_iso(rot["prev_until"]) <= now
            except ValueError:
                over = True
            if over:
                try:
                    self.finalize(w, "grace period ended")
                    n += 1
                except OlcfError:
                    log.exception("closing the credential rotation of %s failed", w.get("name"))
        return n

    def auth_accepts(self, row: dict[str, Any], token_id: str) -> bool:
        """May ``token_id`` authenticate this worker? The current token always; the superseded one during the grace period of a pending rotation."""
        if row.get("token_id") == token_id:
            return True
        rot = _doc(row).get("rotation") or {}
        if rot.get("prev_token_id") == token_id and rot.get("prev_until"):
            try:
                return from_iso(rot["prev_until"]) > utcnow()
            except ValueError:
                return False
        return False

    # ------------------------------------------------------------------ renew_cert (agent -> controller)
    def renew(self, worker_id: str, auth_token_id: str | None, req: dict[str, Any], *, ip: str | None, connected_at: float | None = None) -> dict[str, Any]:
        """Handle one ``renew_cert``. Returns the ``data`` of the ``result`` frame; raises :class:`RenewalRefused` (or another :class:`OlcfError`) for ``result{ok:false}``."""
        with self.registry.db_lock(f"worker-credentials-{worker_id}", ttl=60, wait=15):
            return self._renew_locked(worker_id, auth_token_id, req, ip, connected_at)

    def _renew_locked(self, worker_id: str, auth_token_id: str | None, req: dict[str, Any], ip: str | None, connected_at: float | None) -> dict[str, Any]:
        row = self.registry.get_worker(worker_id)
        if row["state"] == "revoked" or row.get("revoked_at"):
            raise Forbidden("the worker has been revoked")
        pol, now = self.policy(), utcnow()
        doc = _doc(row)
        grant = self._grant(doc, now)
        csr = req.get("csr_pem") or None
        reason = str(req.get("reason") or "scheduled")
        if not grant and not pol.auto_renew:
            raise RenewalRefused("auto_renew_disabled", "automatic credential renewal is switched off (compute.worker_cert_auto_renew)",
                                 remediation="renew this node with `olcf worker renew <worker>`")
        want_cert = bool(csr) and (bool(grant and grant.get("cert")) or self.cert_due(row, pol, now))
        want_token = bool(grant.get("token")) if grant else self.token_due(row, pol, now)
        redeliver = bool(auth_token_id and row.get("token_id") and auth_token_id != row["token_id"] and self._reconnected_with_superseded(doc, connected_at))
        if redeliver:
            want_token = True  # the agent came back with the superseded token after the rotation: it never received the current one, deliver it again
        if not want_cert and not want_token:
            after = self.renew_after(row, pol, now)
            raise RenewalRefused("renewal_not_due", "the certificate is not inside the renewal window yet",
                                 details={"renew_after": after, "cert_expires_at": row.get("cert_expires_at"), "token_expires_at": row.get("token_expires_at"),
                                          "renew_before_days": pol.cert_window_days})
        last = (doc.get("last_renewal") or {}).get("at")
        if not grant and not redeliver and last and (now - from_iso(last)).total_seconds() < MIN_RENEW_INTERVAL_SECONDS:
            raise RenewalRefused("renewal_too_soon", f"credentials were renewed {int((now - from_iso(last)).total_seconds())} s ago",
                                 details={"retry_after_seconds": MIN_RENEW_INTERVAL_SECONDS}, remediation="the agent retries on its own schedule")
        cert_pem = serial = not_after = None
        if want_cert:
            try:
                cert_pem, serial = self.ctx.pki.sign_worker_csr(str(csr), worker_id, days=pol.cert_lifetime_days)
                not_after = cert_not_after(cert_pem)
            except Exception as exc:  # noqa: BLE001 - any parse/verify failure of the CSR
                raise RenewalRefused("bad_csr", f"invalid certificate signing request: {exc}", remediation="send a PEM CSR signed with the node's new key") from exc
        new_token = new_trow = None
        if want_token:
            ttl = int(pol.token_max_age_days * DAY) if pol.token_max_age_days > 0 else None
            new_token, new_trow = self.ctx.tokens.create_worker_token(row["principal_id"], meta={"worker_id": worker_id}, ttl_seconds=ttl)
        rot = dict(doc.get("rotation") or {})
        vals: dict[str, Any] = {"updated_at": now_iso()}
        previous_serial = row.get("cert_serial")
        previous_token = None
        if want_cert:
            if rot.get("prev_serial") and rot["prev_serial"] != previous_serial:
                self.hub.revoke_serial(rot["prev_serial"])  # an earlier swap never completed; this one supersedes it
            rot["prev_serial"] = previous_serial
            vals.update(cert_serial=serial, cert_expires_at=not_after)
        if want_token and new_trow is not None:
            holder = auth_token_id or row.get("token_id")
            for stale in {t for t in (row.get("token_id"), rot.get("prev_token_id")) if t and t != holder}:
                self.ctx.tokens.revoke(str(stale))  # a rotation nobody picked up
            previous_token = holder
            rot["prev_token_id"] = holder
            vals.update(token_id=new_trow["id"], token_expires_at=new_trow.get("expires_at"))
        if rot:
            rot["prev_until"] = iso_in(pol.grace_seconds)
            rot["started_at"] = now_iso()
        doc["rotation"] = rot
        doc.pop("grant", None)
        renewed = (["certificate"] if want_cert else []) + (["token"] if want_token else [])
        doc["last_renewal"] = {"at": now_iso(), "renewed": renewed, "reason": "requested" if grant else reason}
        vals["credentials"] = doc
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(C.workers).where(C.workers.c.id == worker_id, C.workers.c.token_id == row.get("token_id")).values(**vals)).rowcount
        if not n:
            if new_trow is not None:
                self.ctx.tokens.revoke(new_trow["id"])
            raise Conflict("the worker's credentials changed during the renewal", remediation="retry")
        fresh = self.registry.get_worker(worker_id)
        self._announce(fresh, renewed, serial=serial, previous_serial=previous_serial, not_after=not_after, token_row=new_trow, previous_token=previous_token,
                       reason="requested" if grant else reason, ip=ip, by=(grant or {}).get("by"))
        data: dict[str, Any] = {"renewed": renewed, "ca_pem": self.ctx.pki.ca_pem, "cert_expires_at": fresh.get("cert_expires_at"),
                                "token_expires_at": fresh.get("token_expires_at"), "renew_after": self.renew_after(fresh, pol), "server_time": now_iso()}
        if want_cert:
            data.update(cert_pem=cert_pem, serial=serial)
        if want_token:
            data.update(worker_token=new_token, token_grace_seconds=int(pol.grace_seconds))
        return data

    @staticmethod
    def _reconnected_with_superseded(doc: dict[str, Any], connected_at: float | None) -> bool:
        """True when this connection was opened after the pending rotation started yet authenticated with the superseded token (a connection that was already open when the
        new token was handed out legitimately keeps using the old one until it reconnects)."""
        started = (doc.get("rotation") or {}).get("started_at")
        if not started or connected_at is None:
            return False
        try:
            return connected_at > from_iso(started).timestamp()
        except ValueError:
            return False

    def _announce(self, fresh: dict[str, Any], renewed: list[str], *, serial: str | None, previous_serial: str | None, not_after: str | None, token_row: dict[str, Any] | None,
                  previous_token: str | None, reason: str, ip: str | None, by: str | None) -> None:
        actor = Principal(id=fresh.get("principal_id") or "", name=f"worker:{fresh['id']}", kind="worker", token_scopes=["worker"])
        wid, name = fresh["id"], fresh["name"]
        if "certificate" in renewed:
            self.ctx.audit.record(actor, "worker.certificate_renewed", resource_type="worker", resource_id=wid, ip=ip,
                                  details={"name": name, "serial": serial, "previous_serial": previous_serial, "expires_at": not_after, "reason": reason, "requested_by": by})
            self.ctx.events.publish("worker.certificate_renewed", {"worker_id": wid, "name": name, "serial": serial, "expires_at": not_after, "reason": reason}, subject=wid)
        if "token" in renewed and token_row is not None:
            self.ctx.audit.record(actor, "worker.token_rotated", resource_type="worker", resource_id=wid, ip=ip,
                                  details={"name": name, "token_id": token_row["id"], "previous_token_id": previous_token, "expires_at": token_row.get("expires_at"),
                                           "reason": reason, "requested_by": by})
            self.ctx.events.publish("worker.token_rotated", {"worker_id": wid, "name": name, "expires_at": token_row.get("expires_at"), "reason": reason}, subject=wid)

    @staticmethod
    def _grant(doc: dict[str, Any], now: datetime) -> dict[str, Any] | None:
        g = doc.get("grant")
        if not isinstance(g, dict) or not g.get("until"):
            return None
        try:
            return g if from_iso(g["until"]) > now else None
        except ValueError:
            return None

    # ------------------------------------------------------------------ operator command (API)
    def request_renewal(self, principal: Principal, ref: str, *, certificate: bool = True, token: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        """``POST /v1/workers/{id}/renew``: grant a short window and have the agent run the exchange now. Returns what the agent reports plus the worker record."""
        principal.require("compute.manage")
        if not (certificate or token):
            raise BadRequest("nothing to renew: ask for the certificate, the token or both")
        w = self.registry.find_worker(ref)
        if w["state"] == "revoked" or w.get("revoked_at"):
            raise Conflict("the worker is revoked and cannot renew credentials", remediation="enrol the machine again with a new token")
        if not w.get("connected"):
            raise Conflict(f"worker '{w['name']}' is not connected, so it cannot renew now",
                           remediation="start the agent on that machine - it renews on its own when it reconnects inside the renewal window - then run this command again")
        self._set_grant(w["id"], {"until": iso_in(GRANT_SECONDS), "cert": certificate, "token": token, "by": principal.name})
        self.ctx.audit.record(principal, "worker.credentials_renewal_requested", resource_type="worker", resource_id=w["id"],
                              details={"name": w["name"], "certificate": certificate, "token": token})
        try:
            data = self.hub.request(w["id"], "renew_credentials", {"certificate": certificate, "token": token}, timeout=timeout)
        finally:
            self._set_grant(w["id"], None)
        return {"renewed": list((data or {}).get("renewed") or []), "worker_id": w["id"]}

    def _set_grant(self, worker_id: str, grant: dict[str, Any] | None) -> None:
        with self.registry.db_lock(f"worker-credentials-{worker_id}", ttl=60, wait=15):
            row = self.registry.get_worker(worker_id)
            doc = _doc(row)
            if grant:
                doc["grant"] = grant
            else:
                doc.pop("grant", None)
            with self.ctx.db.begin() as conn:
                conn.execute(update(C.workers).where(C.workers.c.id == worker_id).values(credentials=doc))

    # ------------------------------------------------------------------ health
    def register_health(self) -> None:
        self.ctx.health.register(HealthCheck("worker-certificates", "compute", "Worker machine certificates: days until expiry (warn < 30, fail < 7)", self._health, ttl=30))

    def _health(self) -> ComponentHealth:
        rows = expiring_worker_certificates(self.registry, 1e9)
        if not rows:
            return ComponentHealth("worker-certificates", Status.OK, "no worker certificates to track", category="compute", labels={"tracked": 0})
        bad = [r for r in rows if r["days_left"] < WARN_DAYS]
        labels = {"tracked": len(rows), "warn": sum(1 for r in bad if r["days_left"] >= FAIL_DAYS), "fail": sum(1 for r in bad if r["days_left"] < FAIL_DAYS),
                  "min_days_left": rows[0]["days_left"]}
        if not bad:
            return ComponentHealth("worker-certificates", Status.OK, f"{len(rows)} worker certificate(s); the next expires in {rows[0]['days_left']:.0f} days", category="compute", labels=labels)

        def phrase(r: dict[str, Any]) -> str:
            return f"{r['name']} expired {-r['days_left']:.1f} days ago" if r["days_left"] <= 0 else f"{r['name']} expires in {r['days_left']:.1f} days"

        detail = "; ".join(phrase(r) for r in bad[:8]) + (f"; and {len(bad) - 8} more" if len(bad) > 8 else "")
        status = Status.DOWN if labels["fail"] else Status.DEGRADED
        steps = [{"step": r["name"], "ok": False, "detail": phrase(r) + " - agents renew automatically inside the window; `olcf worker renew <name>` renews now"} for r in bad[:20]]
        return ComponentHealth("worker-certificates", status, detail, category="compute", labels=labels, steps=steps)


def public_fields(row: dict[str, Any]) -> dict[str, Any]:
    """The credential fields every worker projection (API, topology) carries."""
    return {"cert_expires_at": row.get("cert_expires_at"), "token_expires_at": row.get("token_expires_at")}


__all__ = ["MIN_RENEW_INTERVAL_SECONDS", "Policy", "RenewalRefused", "WorkerCredentials", "expiring_worker_certificates", "public_fields"]
