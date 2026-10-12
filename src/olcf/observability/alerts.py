# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Alerting (spec 28): rules, evaluation, notification channels and the alert history.

Model
-----
``alert_rules``     what to watch (``type`` + ``params``), how long it must hold (``for_seconds``), ``severity``, where to notify (``channels``), ``repeat_seconds``.
``alerts``          one row per *firing episode* (``rule_id`` + ``instance_key``) - this table is the history. States ``pending`` (condition true, ``for`` not yet elapsed),
                    ``firing`` and ``resolved``. At most one active (pending / firing) row exists per ``(rule, instance)``: that is the de-duplication.
``alert_channels``  ``log``, ``webhook`` (signed JSON POST through the SSRF guard) and ``email`` (SMTP, configured with the ``alerts.smtp.*`` platform settings).

Evaluation is leader-only (``ctx.leader.is_leader()``), reads the database directly (so it is independent of which replica holds which in-memory metric) and is
idempotent: running it twice in a row changes nothing. A condition that disappears resolves the alert after ``params.resolve_after_seconds`` (default 0), an acknowledged
alert stops repeating, a silenced rule / alert keeps its state but sends nothing. Every transition also publishes an ``alert.firing`` / ``alert.resolved`` /
``alert.acknowledged`` event, so outbound webhooks (spec 24) can subscribe to ``alert.*`` as well.

Rule types (``GET /v1/alerts/rule-types`` returns the parameter documentation)
    ``failed_jobs``  ``failed_pipelines``  ``queue_saturation``  ``worker_loss``  ``disk_pressure``  ``runtime_mismatch``  ``auth_expiry``  ``control_plane_health``
    ``fabric_handshake_stale``
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import smtplib
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, Forbidden, NotFound, OlcfError, ValidationFailed
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import from_iso, to_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, paginate
from olcf.pipelines import ssrf
from olcf.pipelines.common import LeaderLoop, mask_url, setting
from olcf.pipelines.engine import lid
from olcf.pipelines.schema import alert_channels as CH
from olcf.pipelines.schema import alert_rules as AR
from olcf.pipelines.schema import alerts as AL
from olcf.pipelines.webhooks import sign

log = logging.getLogger("olcf.observability.alerts")

SEVERITIES = ("info", "warning", "critical")
_EMAIL = re.compile(r"^[^@\s,;]{1,64}@[^@\s,;]{1,255}$")
MAX_INSTANCES = 200  # per rule evaluation: a runaway rule must not create thousands of alerts


@dataclass
class Condition:
    """One thing that is wrong right now (an instance of a rule)."""

    key: str
    summary: str
    value: float | None = None
    labels: dict[str, str] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    workspace_id: str | None = None


# ---------------------------------------------------------------------------------------------------------------------------------------- rule type catalogue
def _num(lo: float, hi: float, default: float) -> dict[str, Any]:
    return {"type": "number", "min": lo, "max": hi, "default": default}


def _int(lo: int, hi: int, default: int) -> dict[str, Any]:
    return {"type": "integer", "min": lo, "max": hi, "default": default}


def _str(default: str | None = None, choices: list[str] | None = None) -> dict[str, Any]:
    return {"type": "string", "default": default, "choices": choices}


def _strs() -> dict[str, Any]:
    return {"type": "string_list", "default": []}


COMMON = {"resolve_after_seconds": _int(0, 86400, 0), "interval_seconds": _int(5, 3600, 30)}
RULE_TYPES: dict[str, dict[str, Any]] = {
    "failed_jobs": {"title": "Failed jobs", "workspace": True, "description": "Jobs that failed or timed out within a window reach a threshold.",
                    "params": {"window_seconds": _int(60, 604800, 900), "threshold": _int(1, 100000, 5), "kind": _str(), "queue": _str(), "group_by": _str("none", ["none", "queue", "workspace"])}},
    "failed_pipelines": {"title": "Failed pipeline runs", "workspace": True, "description": "Pipeline runs that failed within a window reach a threshold.",
                         "params": {"window_seconds": _int(60, 604800, 3600), "threshold": _int(1, 100000, 3), "pipeline_id": _str(), "group_by": _str("none", ["none", "pipeline"])}},
    "queue_saturation": {"title": "Queue saturation", "workspace": False, "description": "A queue's depth reaches a fraction of its limit, or its oldest job waits too long.",
                         "params": {"ratio": _num(0.01, 1.0, 0.9), "min_depth": _int(1, 1000000, 10), "oldest_wait_seconds": _int(0, 604800, 900), "queue": _str()}},
    "worker_loss": {"title": "Worker lost", "workspace": False, "description": "A worker is offline or has stopped sending heartbeats.",
                    "params": {"grace_seconds": _int(10, 86400, 60), "ignore_draining": {"type": "boolean", "default": True}, "pool": _str()}},
    "disk_pressure": {"title": "Disk pressure", "workspace": False, "description": "A worker (or the control-plane data directory) is running out of disk space.",
                      "params": {"used_percent": _num(1, 100, 90), "include_control_plane": {"type": "boolean", "default": True}}},
    "runtime_mismatch": {"title": "Runtime mismatch", "workspace": False, "description": "A runtime failed to install or verify on a worker, or workers report different fingerprints for it.",
                         "params": {"runtime": _str()}},
    "auth_expiry": {"title": "Credential / certificate expiry", "workspace": False, "description": "API tokens, the platform certificates or worker machine certificates expire soon.",
                    "params": {"within_days": _num(0, 3650, 14), "kinds": {"type": "string_list", "default": ["pat", "worker"]}, "check_certificates": {"type": "boolean", "default": True},
                               "check_worker_certificates": {"type": "boolean", "default": True}, "worker_certificate_days": _num(0, 3650, 30)}},
    "control_plane_health": {"title": "Control-plane health", "workspace": False, "description": "A health check (database, leader, scheduler, ...) is degraded or down.",
                            "params": {"min_status": _str("down", ["degraded", "down"]), "components": _strs(), "categories": _strs()}},
    "fabric_handshake_stale": {"title": "Stale Fabric handshake", "workspace": False, "description": "No successful Microsoft Fabric handshake for a while.",
                               "params": {"max_age_seconds": _int(60, 31536000, 3600), "require_handshake": {"type": "boolean", "default": False}}},
}


def validate_params(rule_type: str, params: Any) -> dict[str, Any]:
    spec = RULE_TYPES.get(rule_type)
    if spec is None:
        raise ValidationFailed(f"unknown alert rule type '{rule_type}'", details={"types": sorted(RULE_TYPES)})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise ValidationFailed("params must be an object")
    allowed = {**spec["params"], **COMMON}
    extra = sorted(set(params) - set(allowed))
    if extra:
        raise ValidationFailed(f"unknown parameter(s) {extra} for rule type '{rule_type}'", details={"allowed": sorted(allowed)})
    out: dict[str, Any] = {}
    for k, d in allowed.items():
        v = params.get(k, d.get("default"))
        t = d["type"]
        if v is None:
            out[k] = None
            continue
        if t in ("number", "integer"):
            if isinstance(v, bool) or not isinstance(v, (int, float)) or (t == "integer" and int(v) != v):
                raise ValidationFailed(f"parameter '{k}' must be {'an integer' if t == 'integer' else 'a number'}")
            if not (d["min"] <= v <= d["max"]):
                raise ValidationFailed(f"parameter '{k}' must be between {d['min']} and {d['max']}")
            v = int(v) if t == "integer" else float(v)
        elif t == "string":
            if not isinstance(v, str) or len(v) > 200:
                raise ValidationFailed(f"parameter '{k}' must be a string")
            if d.get("choices") and v not in d["choices"]:
                raise ValidationFailed(f"parameter '{k}' must be one of {d['choices']}")
        elif t == "boolean":
            if not isinstance(v, bool):
                raise ValidationFailed(f"parameter '{k}' must be true or false")
        elif t == "string_list" and (not isinstance(v, list) or not all(isinstance(x, str) and len(x) <= 200 for x in v) or len(v) > 100):
            raise ValidationFailed(f"parameter '{k}' must be a list of strings")
        out[k] = v
    return out


# ---------------------------------------------------------------------------------------------------------------------------------------- evaluators
class Evaluators:
    """Condition producers, one method per rule type. They only *read*; state handling lives in :class:`AlertService`."""

    def __init__(self, ctx: Any, clock: Callable[[], datetime]) -> None:
        self.ctx, self.db, self.now = ctx, ctx.db, clock

    def run(self, rule: dict[str, Any]) -> list[Condition]:
        fn = getattr(self, "_" + rule["type"], None)
        if fn is None:
            raise BadRequest(f"no evaluator for rule type '{rule['type']}'")
        return fn(rule, rule["params"] or {})[:MAX_INSTANCES]

    # -- jobs / pipelines
    def _failed_jobs(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.compute.schema import jobs as J

        since = to_iso(self.now() - timedelta(seconds=int(p.get("window_seconds") or 900)))
        stmt = select(J.c.id, J.c.workspace_id, J.c.queue, J.c.kind, J.c.failure_category, J.c.name).where(J.c.state.in_(("failed", "timed_out")), J.c.finished_at >= since).order_by(J.c.finished_at.desc()).limit(5000)
        if rule.get("workspace_id"):
            stmt = stmt.where(J.c.workspace_id == rule["workspace_id"])
        if p.get("kind"):
            stmt = stmt.where(J.c.kind == p["kind"])
        if p.get("queue"):
            stmt = stmt.where(J.c.queue == p["queue"])
        with self.db.connect() as conn:
            rows = fetch_all(conn, stmt)
        group = p.get("group_by") or "none"
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            groups.setdefault("all" if group == "none" else str(r[group]), []).append(r)
        thr = int(p.get("threshold") or 1)
        out = []
        mins = int(p.get("window_seconds") or 900) // 60
        for key, rs in sorted(groups.items()):
            if len(rs) >= thr:
                cats: dict[str, int] = {}
                for r in rs:
                    cats[r["failure_category"] or "unknown"] = cats.get(r["failure_category"] or "unknown", 0) + 1
                label = {} if group == "none" else {group: key}
                out.append(Condition(key if group != "none" else "all", f"{len(rs)} job(s) failed in the last {mins} min" + (f" ({group}={key})" if group != "none" else ""), float(len(rs)), label,
                                     {"count": len(rs), "threshold": thr, "failure_categories": cats, "job_ids": [r["id"] for r in rs[:10]]},
                                     rule.get("workspace_id") or (key if group == "workspace" else None)))
        return out

    def _failed_pipelines(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.pipelines.schema import pipeline_runs as R

        since = to_iso(self.now() - timedelta(seconds=int(p.get("window_seconds") or 3600)))
        stmt = select(R.c.id, R.c.workspace_id, R.c.pipeline_id, R.c.pipeline_name, R.c.state_reason).where(R.c.state == "failed", R.c.finished_at >= since).order_by(R.c.finished_at.desc()).limit(5000)
        if rule.get("workspace_id"):
            stmt = stmt.where(R.c.workspace_id == rule["workspace_id"])
        if p.get("pipeline_id"):
            stmt = stmt.where(R.c.pipeline_id == p["pipeline_id"])
        with self.db.connect() as conn:
            rows = fetch_all(conn, stmt)
        group = p.get("group_by") or "none"
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            groups.setdefault("all" if group == "none" else r["pipeline_id"], []).append(r)
        thr = int(p.get("threshold") or 1)
        out = []
        mins = int(p.get("window_seconds") or 3600) // 60
        for key, rs in sorted(groups.items()):
            if len(rs) >= thr:
                name = rs[0]["pipeline_name"] if group == "pipeline" else None
                out.append(Condition(key, f"{len(rs)} pipeline run(s) failed in the last {mins} min" + (f" ({name})" if name else ""), float(len(rs)), {"pipeline": name} if name else {},
                                     {"count": len(rs), "threshold": thr, "run_ids": [r["id"] for r in rs[:10]], "last_reason": redact_text(rs[0]["state_reason"] or "")[:200]},
                                     rule.get("workspace_id") or rs[0]["workspace_id"] if group == "pipeline" else rule.get("workspace_id")))
        return out

    # -- compute
    def _queue_saturation(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.compute.schema import jobs as J
        from olcf.compute.schema import queues as Q

        now = self.now()
        with self.db.connect() as conn:
            queues = fetch_all(conn, select(Q.c.name, Q.c.max_queued))
            queued = fetch_all(conn, select(J.c.queue, J.c.queued_at).where(J.c.state == "queued"))
        depth: dict[str, int] = {}
        oldest: dict[str, float] = {}
        for r in queued:
            depth[r["queue"]] = depth.get(r["queue"], 0) + 1
            if r["queued_at"]:
                oldest[r["queue"]] = max(oldest.get(r["queue"], 0.0), (now - from_iso(r["queued_at"])).total_seconds())
        limits = {q["name"]: q["max_queued"] for q in queues}
        out = []
        for name in sorted(set(limits) | set(depth)):
            if p.get("queue") and name != p["queue"]:
                continue
            d, w, lim = depth.get(name, 0), oldest.get(name, 0.0), limits.get(name)
            reasons = []
            ratio = d / lim if lim else None
            if ratio is not None and ratio >= float(p.get("ratio") or 0.9) and d >= int(p.get("min_depth") or 1):
                reasons.append(f"{d} of {lim} queue slots used ({ratio:.0%})")
            wait_limit = int(p.get("oldest_wait_seconds") or 0)
            if wait_limit and w >= wait_limit and d >= 1:
                reasons.append(f"oldest job has waited {int(w)} s")
            if reasons:
                out.append(Condition(name, f"queue '{name}': " + "; ".join(reasons), float(d), {"queue": name}, {"depth": d, "max_queued": lim, "oldest_wait_seconds": round(w, 1)}))
        return out

    def _worker_loss(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.compute.schema import workers as W

        now = self.now()
        grace = float(p.get("grace_seconds") or 60)
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(W.c.id, W.c.name, W.c.pool, W.c.state, W.c.scheduling, W.c.last_seen_at, W.c.connected).where(W.c.revoked_at.is_(None), W.c.state != "pending"))
        out = []
        for r in rows:
            if p.get("pool") and r["pool"] != p["pool"]:
                continue
            if p.get("ignore_draining", True) and r["scheduling"] == "draining":
                continue
            age = (now - from_iso(r["last_seen_at"])).total_seconds() if r["last_seen_at"] else None
            lost = r["state"] == "offline" or (age is not None and age > grace and not r["connected"])
            if lost and (age is None or age >= grace):
                out.append(Condition(r["name"], f"worker '{r['name']}' is {r['state']}" + (f" (no heartbeat for {int(age)} s)" if age is not None else ""), age, {"worker": r["name"], "pool": r["pool"]},
                                     {"worker_id": r["id"], "state": r["state"], "last_seen_at": r["last_seen_at"]}))
        return out

    def _disk_pressure(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.compute.schema import workers as W

        limit = float(p.get("used_percent") or 90)
        out = []
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(W.c.name, W.c.pool, W.c.metrics).where(W.c.revoked_at.is_(None), W.c.state.in_(("online", "degraded"))))
        for r in rows:
            m = r["metrics"] or {}
            if m.get("disk_total_gb") and m.get("disk_free_gb") is not None:
                used = 100.0 * (1.0 - float(m["disk_free_gb"]) / float(m["disk_total_gb"]))
                if used >= limit:
                    out.append(Condition(r["name"], f"worker '{r['name']}' disk is {used:.1f}% full ({float(m['disk_free_gb']):.1f} GB free)", round(used, 2), {"worker": r["name"], "pool": r["pool"]},
                                         {"disk_free_gb": m["disk_free_gb"], "disk_total_gb": m["disk_total_gb"]}))
        if p.get("include_control_plane", True):
            try:
                du = shutil.disk_usage(self.ctx.settings.data_dir)
                used = 100.0 * (du.used / du.total) if du.total else 0.0
                if used >= limit:
                    out.append(Condition("control-plane", f"control-plane data directory is {used:.1f}% full ({du.free / 1e9:.1f} GB free)", round(used, 2), {"worker": "control-plane"},
                                         {"free_gb": round(du.free / 1e9, 2), "total_gb": round(du.total / 1e9, 2)}))
            except OSError:
                pass
        return out

    def _runtime_mismatch(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        from olcf.compute.schema import runtime_installs as RI
        from olcf.compute.schema import workers as W

        with self.db.connect() as conn:
            installs = fetch_all(conn, select(RI))
            workers = {w["id"]: w["name"] for w in fetch_all(conn, select(W.c.id, W.c.name).where(W.c.revoked_at.is_(None)))}
        out = []
        by_runtime: dict[str, list[dict[str, Any]]] = {}
        for i in installs:
            if i["worker_id"] not in workers or (p.get("runtime") and i["runtime_id"] != p["runtime"]):
                continue
            by_runtime.setdefault(i["runtime_id"], []).append(i)
        for rid, ins in sorted(by_runtime.items()):
            for i in ins:
                w = workers[i["worker_id"]]
                if i["state"] == "failed":
                    out.append(Condition(f"{rid}@{w}", f"runtime {rid} failed to install on worker '{w}': {redact_text(str(i.get('detail') or ''))[:150]}", None, {"runtime": rid, "worker": w},
                                         {"state": i["state"]}))
                elif i["state"] == "ready" and not i["verified"]:
                    out.append(Condition(f"{rid}@{w}", f"runtime {rid} on worker '{w}' is installed but not verified", None, {"runtime": rid, "worker": w}, {"state": i["state"]}))
            fps = {i["fingerprint"] for i in ins if i["state"] == "ready" and i["fingerprint"]}
            if len(fps) > 1:
                out.append(Condition(f"{rid}@fingerprint", f"workers report {len(fps)} different fingerprints for runtime {rid}", float(len(fps)), {"runtime": rid},
                                     {"workers": sorted(workers[i["worker_id"]] for i in ins if i["state"] == "ready")}))
        return out

    # -- security
    def _auth_expiry(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        now = self.now()
        horizon = now + timedelta(days=float(p.get("within_days") if p.get("within_days") is not None else 14))
        out: list[Condition] = []
        kinds = list(p.get("kinds") or ["pat", "worker"])
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(S.api_tokens.c.id, S.api_tokens.c.name, S.api_tokens.c.prefix, S.api_tokens.c.kind, S.api_tokens.c.expires_at, S.api_tokens.c.principal_id)
                             .where(S.api_tokens.c.revoked_at.is_(None), S.api_tokens.c.expires_at.is_not(None), S.api_tokens.c.expires_at <= to_iso(horizon),
                                    S.api_tokens.c.expires_at >= to_iso(now - timedelta(days=1)), S.api_tokens.c.kind.in_(kinds or [""])).order_by(S.api_tokens.c.expires_at.asc()).limit(MAX_INSTANCES))
        for r in rows:
            days = (from_iso(r["expires_at"]) - now).total_seconds() / 86400
            when = f"in {days:.1f} days" if days > 0 else "just expired"
            out.append(Condition(f"token:{r['id']}", f"{r['kind']} token '{r['name'] or r['prefix']}' expires {when}", round(days, 2), {"kind": r["kind"]},
                                 {"token_id": r["id"], "principal_id": r["principal_id"], "expires_at": r["expires_at"]}))
        if p.get("check_certificates", True):
            out += self._certs(now, horizon)
        if p.get("check_worker_certificates", True):
            out += self._worker_certs(now, float(p["worker_certificate_days"]) if p.get("worker_certificate_days") is not None else 30.0)
        return out

    def _worker_certs(self, now: datetime, within_days: float) -> list[Condition]:
        """Worker machine certificates that expire within ``within_days`` (agents renew inside the last 30 days, so a hit means a node that is offline or cannot renew)."""
        try:
            from olcf.compute.schema import workers as W
        except ImportError:
            return []
        horizon = to_iso(now + timedelta(days=within_days))
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(W.c.id, W.c.name, W.c.pool, W.c.state, W.c.cert_expires_at).where(
                W.c.revoked_at.is_(None), W.c.cert_expires_at.is_not(None), W.c.cert_expires_at <= horizon).order_by(W.c.cert_expires_at.asc()).limit(MAX_INSTANCES))
        out = []
        for r in rows:
            days = (from_iso(r["cert_expires_at"]) - now).total_seconds() / 86400
            when = f"expires in {days:.1f} days" if days > 0 else "has expired"
            out.append(Condition(f"worker-cert:{r['id']}", f"the machine certificate of worker '{r['name']}' {when} ({r['state']})", round(days, 2), {"worker": r["name"], "pool": r["pool"]},
                                 {"worker_id": r["id"], "state": r["state"], "not_after": r["cert_expires_at"]}))
        return out

    def _certs(self, now: datetime, horizon: datetime) -> list[Condition]:
        out = []
        try:
            from cryptography import x509

            pki = Path(self.ctx.settings.path("pki"))
            for label, fname in (("server", "server.crt"), ("ca", "ca.crt")):
                f = pki / fname
                if not f.exists():
                    continue
                cert = x509.load_pem_x509_certificate(f.read_bytes())
                end = cert.not_valid_after_utc
                if end <= horizon:
                    days = (end - now).total_seconds() / 86400
                    out.append(Condition(f"cert:{label}", f"the platform {label} certificate expires " + (f"in {days:.1f} days" if days > 0 else "(already expired)"), round(days, 2), {"certificate": label},
                                         {"not_after": to_iso(end), "subject": cert.subject.rfc4514_string()}))
        except Exception:  # noqa: BLE001 - an unreadable certificate is reported by the 'pki' health check
            log.debug("certificate check failed", exc_info=True)
        return out

    def _control_plane_health(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        snap = self.ctx.health.snapshot()
        want_down_only = (p.get("min_status") or "down") == "down"
        comps = set(p.get("components") or [])
        cats = set(p.get("categories") or [])
        out = []
        for c in snap["components"]:
            if comps and c["name"] not in comps:
                continue
            if cats and c.get("category") not in cats:
                continue
            bad = c["status"] == "down" or (c["status"] == "degraded" and not want_down_only)
            if bad:
                out.append(Condition(c["name"], f"health check '{c['name']}' is {c['status']}: {redact_text(str(c.get('detail') or ''))[:200]}", None, {"component": c["name"], "category": str(c.get("category"))},
                                     {"status": c["status"], "detail": redact_text(str(c.get("detail") or ""))[:500]}))
        return out

    def _fabric_handshake_stale(self, rule: dict[str, Any], p: dict[str, Any]) -> list[Condition]:
        try:
            from olcf.fabric.schema import fabric_handshakes as H
        except ImportError:
            return []
        with self.db.connect() as conn:
            last_ok = conn.execute(select(func.max(H.c.received_at)).where(H.c.ok.is_(True))).scalar()
            ever = conn.execute(select(func.count()).select_from(H)).scalar() or 0
        if not last_ok:
            if ever or p.get("require_handshake"):
                return [Condition("fabric", "no successful Microsoft Fabric handshake has ever been received" if not ever else "no successful Microsoft Fabric handshake yet (only failed ones)", None, {})]
            return []
        age = (self.now() - from_iso(last_ok)).total_seconds()
        if age > float(p.get("max_age_seconds") or 3600):
            return [Condition("fabric", f"the last successful Fabric handshake was {int(age // 60)} min ago", round(age, 1), {}, {"last_success_at": last_ok})]
        return []


# ---------------------------------------------------------------------------------------------------------------------------------------- notifications
def _public_alert(row: dict[str, Any], rule_name: str | None = None, now: datetime | None = None) -> dict[str, Any]:
    out = {k: row.get(k) for k in ("id", "rule_id", "instance_key", "workspace_id", "severity", "state", "summary", "value", "labels", "started_at", "fired_at", "resolved_at", "last_seen_at",
                                    "last_notified_at", "notify_count", "acknowledged_at", "acknowledged_by", "silenced_until", "fingerprint")}
    out["details"] = redact_obj(row.get("details") or {})
    if rule_name is not None:
        out["rule_name"] = rule_name
    try:
        end = from_iso(row["resolved_at"]) if row.get("resolved_at") else (now or datetime.now(UTC))
        out["duration_seconds"] = max(0.0, (end - from_iso(row["started_at"])).total_seconds())
    except Exception:  # noqa: BLE001
        out["duration_seconds"] = None
    return out


class Notifier:
    """Delivers one notification to a channel. ``send`` returns ``{ok, detail}`` and never raises."""

    def __init__(self, ctx: Any, transport: httpx.BaseTransport | None = None) -> None:
        self.ctx = ctx
        self._transport = transport

    def send(self, channel: dict[str, Any], payload: dict[str, Any], *, attempts: int = 3) -> dict[str, Any]:
        try:
            if channel["type"] == "log":
                return self._log(channel, payload)
            if channel["type"] == "webhook":
                return self._webhook(channel, payload, attempts)
            if channel["type"] == "email":
                return self._email(channel, payload)
            return {"ok": False, "detail": f"unknown channel type {channel['type']}"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "detail": f"{type(exc).__name__}: {redact_text(str(exc))[:300]}"}

    @staticmethod
    def _log(channel: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        level = logging.ERROR if payload["alert"]["severity"] == "critical" else logging.WARNING if payload["status"] == "firing" else logging.INFO
        log.log(level, "ALERT %s [%s] %s - %s", payload["status"].upper(), payload["alert"]["severity"], payload["rule"]["name"], payload["alert"]["summary"])
        return {"ok": True, "detail": "logged"}

    def _webhook(self, channel: dict[str, Any], payload: dict[str, Any], attempts: int) -> dict[str, Any]:
        url = (channel.get("config") or {}).get("url")
        body = json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True).encode()
        ts = str(int(time.time()))
        headers = {"Content-Type": "application/json", "User-Agent": "olcf-alerts/1", "X-OLCF-Event": f"alert.{payload['status']}", "X-OLCF-Timestamp": ts, **((channel.get("config") or {}).get("headers") or {})}
        if channel.get("secret_id"):
            secret = self.ctx.secrets.resolve(channel["secret_id"])
            headers["X-OLCF-Signature"] = sign(secret if isinstance(secret, str) else json.dumps(secret), ts, body)
        policy = ssrf.policy_from_ctx(self.ctx)
        last = "no attempt"
        for n in range(1, attempts + 1):
            try:
                ssrf.validate_url(url, policy)
                client = httpx.Client(transport=self._transport, timeout=10.0, trust_env=False) if self._transport is not None else ssrf.guarded_client(policy, timeout=10.0)
                with client:
                    resp = client.post(url, content=body, headers=headers)
                if 200 <= resp.status_code < 300:
                    return {"ok": True, "detail": f"HTTP {resp.status_code}", "attempts": n}
                last = f"HTTP {resp.status_code}"
                if resp.status_code < 500 and resp.status_code not in (408, 429):
                    break
            except OlcfError as exc:
                return {"ok": False, "detail": f"blocked: {exc.message}"}
            except httpx.HTTPError as exc:
                last = f"{type(exc).__name__}: {redact_text(str(exc))[:150]}"
            if n < attempts:
                time.sleep(min(0.5 * (3 ** (n - 1)), 5.0))
        return {"ok": False, "detail": last, "attempts": attempts}

    def _email(self, channel: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        cfg = channel.get("config") or {}
        host = setting(self.ctx, "alerts.smtp.host", "")
        if not host:
            return {"ok": False, "detail": "SMTP is not configured (set the alerts.smtp.* platform settings)"}
        port = int(setting(self.ctx, "alerts.smtp.port", 587))
        user = setting(self.ctx, "alerts.smtp.username", "")
        pw_secret = setting(self.ctx, "alerts.smtp.password_secret", "")
        sender = setting(self.ctx, "alerts.smtp.from", "") or f"olcf@{host}"
        use_ssl = bool(setting(self.ctx, "alerts.smtp.ssl", False))
        starttls = bool(setting(self.ctx, "alerts.smtp.starttls", True))
        timeout = float(setting(self.ctx, "alerts.smtp.timeout_seconds", 15))
        a, rule = payload["alert"], payload["rule"]
        msg = EmailMessage()
        msg["From"], msg["To"] = sender, ", ".join(cfg.get("to") or [])
        msg["Subject"] = f"{cfg.get('subject_prefix') or '[OLCF]'} {payload['status'].upper()} {a['severity']}: {rule['name']}"[:200].replace("\n", " ")
        msg.set_content(f"{a['summary']}\n\nRule: {rule['name']} ({rule['type']})\nSeverity: {a['severity']}\nState: {payload['status']}\nStarted: {a['started_at']}\n"
                        f"Instance: {a['instance_key']}\n\nDetails:\n{json.dumps(a.get('details') or {}, indent=2, default=str)[:4000]}\n")
        cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with cls(host, port, timeout=timeout) as smtp:
            if starttls and not use_ssl:
                smtp.starttls()
            if user:
                pw = self.ctx.secrets.resolve(pw_secret) if pw_secret else ""
                smtp.login(user, pw if isinstance(pw, str) else str(pw.get("password", "")))
            smtp.send_message(msg)
        return {"ok": True, "detail": f"sent to {len(cfg.get('to') or [])} recipient(s)"}


# ---------------------------------------------------------------------------------------------------------------------------------------- service
DEFAULT_RULES: list[dict[str, Any]] = [
    {"builtin_key": "failed-jobs", "name": "Failed jobs", "type": "failed_jobs", "severity": "warning", "for_seconds": 0, "params": {"window_seconds": 900, "threshold": 5},
     "description": "5 or more jobs failed or timed out within 15 minutes."},
    {"builtin_key": "failed-pipelines", "name": "Failed pipeline runs", "type": "failed_pipelines", "severity": "warning", "for_seconds": 0, "params": {"window_seconds": 3600, "threshold": 3},
     "description": "3 or more pipeline runs failed within an hour."},
    {"builtin_key": "queue-saturation", "name": "Queue saturation", "type": "queue_saturation", "severity": "warning", "for_seconds": 120, "params": {"ratio": 0.9, "oldest_wait_seconds": 900},
     "description": "A queue is at 90% of its limit or its oldest job has waited 15 minutes."},
    {"builtin_key": "worker-lost", "name": "Worker lost", "type": "worker_loss", "severity": "critical", "for_seconds": 60, "params": {"grace_seconds": 60},
     "description": "A worker is offline or stopped sending heartbeats."},
    {"builtin_key": "disk-pressure", "name": "Disk pressure", "type": "disk_pressure", "severity": "warning", "for_seconds": 300, "params": {"used_percent": 90},
     "description": "A worker or the control-plane data directory is more than 90% full."},
    {"builtin_key": "disk-critical", "name": "Disk almost full", "type": "disk_pressure", "severity": "critical", "for_seconds": 60, "params": {"used_percent": 97},
     "description": "A worker or the control-plane data directory is more than 97% full."},
    {"builtin_key": "runtime-mismatch", "name": "Runtime mismatch", "type": "runtime_mismatch", "severity": "warning", "for_seconds": 300, "params": {},
     "description": "A runtime failed to install or verify on a worker, or workers disagree about its fingerprint."},
    {"builtin_key": "auth-expiry", "name": "Credentials and certificates expiring", "type": "auth_expiry", "severity": "warning", "for_seconds": 0, "params": {"within_days": 14},
     "description": "API tokens or platform certificates expire within 14 days, or a worker machine certificate within 30 days."},
    {"builtin_key": "worker-certificate-critical", "name": "Worker certificate about to expire", "type": "auth_expiry", "severity": "critical", "for_seconds": 0,
     "params": {"kinds": [], "check_certificates": False, "check_worker_certificates": True, "worker_certificate_days": 7},
     "description": "A worker machine certificate expires within 7 days (or has expired): the node will be refused by mTLS unless it renews or is re-enrolled."},
    {"builtin_key": "control-plane-health", "name": "Control-plane component down", "type": "control_plane_health", "severity": "critical", "for_seconds": 120, "params": {"min_status": "down"},
     "description": "A platform health check reports 'down'."},
    {"builtin_key": "fabric-handshake-stale", "name": "Stale Fabric handshake", "type": "fabric_handshake_stale", "severity": "warning", "for_seconds": 0, "params": {"max_age_seconds": 3600}, "enabled": False,
     "description": "No successful Microsoft Fabric handshake for an hour (enable when the Fabric integration is in use)."},
]


class AlertService:
    """Rules, channels, evaluation and notification (see module docstring). Registered as ``ctx.service("alerts")``."""

    def __init__(self, ctx: Any, *, clock: Callable[[], datetime] | None = None, transport: httpx.BaseTransport | None = None, tick_seconds: float = 5.0) -> None:
        self.ctx, self.db = ctx, ctx.db
        self._clock = clock
        self.evaluators = Evaluators(ctx, self.now)
        self.notifier = Notifier(ctx, transport)
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="olcf-alert")
        self._pending: list[Future[Any]] = []
        self._lock = threading.Lock()
        self.loop = LeaderLoop(ctx, "alerts", self.evaluate_all, tick_seconds, log)
        self.last_run: dict[str, Any] = {}

    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def now_iso(self) -> str:
        return to_iso(self.now())

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle / settings
    def register_settings(self) -> None:
        st = self.ctx.maybe_service("settings")
        if st is None or "alerts.smtp.host" in st.registry:
            return

        def s(max_len: int = 255) -> Any:
            def v(x: Any) -> Any:
                if x is None:
                    return ""
                if not isinstance(x, str) or len(x) > max_len:
                    raise BadRequest(f"must be a string of at most {max_len} characters")
                return x

            return v

        def port(x: Any) -> Any:
            if isinstance(x, bool) or not isinstance(x, int) or not 1 <= x <= 65535:
                raise BadRequest("must be a port number")
            return x

        def flag(x: Any) -> Any:
            if not isinstance(x, bool):
                raise BadRequest("must be true or false")
            return x

        def secs(x: Any) -> Any:
            if isinstance(x, bool) or not isinstance(x, (int, float)) or not 1 <= x <= 300:
                raise BadRequest("must be between 1 and 300")
            return x

        st.register("alerts.smtp.host", description="SMTP relay used by email alert channels (empty = email disabled)", default="", validator=s())
        st.register("alerts.smtp.port", description="SMTP port", default=587, validator=port)
        st.register("alerts.smtp.username", description="SMTP user name (empty = no authentication)", default="", validator=s())
        st.register("alerts.smtp.password_secret", description="Id of the secret holding the SMTP password", default="", validator=s())
        st.register("alerts.smtp.from", description="From address of alert e-mails", default="", validator=s())
        st.register("alerts.smtp.starttls", description="Upgrade the SMTP connection with STARTTLS", default=True, validator=flag)
        st.register("alerts.smtp.ssl", description="Connect with implicit TLS (SMTPS, usually port 465)", default=False, validator=flag)
        st.register("alerts.smtp.timeout_seconds", description="SMTP connection timeout", default=15, validator=secs)
        def days(x: Any) -> Any:
            if isinstance(x, bool) or not isinstance(x, int) or not 1 <= x <= 3650:
                raise BadRequest("must be an integer between 1 and 3650")
            return x

        st.register("alerts.history_days", description="Days to keep resolved alerts", default=90, validator=days)

    def start(self) -> None:
        self.ensure_default_rules()
        self.loop.start()

    def stop(self) -> None:
        self.loop.stop()
        self._pool.shutdown(wait=True, cancel_futures=True)

    # ------------------------------------------------------------------------------------------------------------------------------ authorisation
    @staticmethod
    def authorize(principal: Any, workspace_id: str | None) -> None:
        if workspace_id:
            principal.require("alert.manage", workspace_id=workspace_id)
        else:
            principal.require("alert.manage")

    @staticmethod
    def can_read(principal: Any, workspace_id: str | None) -> bool:
        return principal.can("events.read", workspace_id=workspace_id) if workspace_id else principal.can("events.read")

    # ------------------------------------------------------------------------------------------------------------------------------ rules
    def ensure_default_rules(self) -> int:
        """Create the shipped rules that do not exist yet (never touches edited ones). Safe on every replica."""
        n = 0
        now = self.now_iso()
        with self.db.connect() as conn:
            have = {r[0] for r in conn.execute(select(AR.c.builtin_key).where(AR.c.builtin_key.is_not(None))).all()}
        for d in DEFAULT_RULES:
            if d["builtin_key"] in have:
                continue
            row = {"id": lid("arl"), "workspace_id": None, "name": d["name"], "description": d["description"], "type": d["type"], "severity": d["severity"], "params": validate_params(d["type"], d["params"]),
                   "for_seconds": d["for_seconds"], "repeat_seconds": 3600, "channels": [], "labels": {}, "enabled": d.get("enabled", True), "builtin_key": d["builtin_key"], "etag": lid("etg")[-16:],
                   "created_at": now, "updated_at": now}
            try:
                with self.db.begin() as conn:
                    conn.execute(AR.insert().values(**row))
                n += 1
            except IntegrityError:
                pass
        return n

    def _validate_rule(self, body: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
        merged = {**(existing or {}), **body}
        typ = merged.get("type")
        if existing and "type" in body and body["type"] != existing["type"]:
            raise ValidationFailed("the type of a rule cannot be changed; create a new rule")
        name = str(merged.get("name") or "").strip()
        if not name or len(name) > 128:
            raise ValidationFailed("name is required (1-128 characters)")
        sev = merged.get("severity") or "warning"
        if sev not in SEVERITIES:
            raise ValidationFailed(f"severity must be one of {', '.join(SEVERITIES)}")
        ws = merged.get("workspace_id") or None
        if typ not in RULE_TYPES:
            raise ValidationFailed(f"unknown alert rule type '{typ}'", details={"types": sorted(RULE_TYPES)})
        if ws and not RULE_TYPES[typ]["workspace"]:
            raise ValidationFailed(f"rule type '{typ}' is platform-wide and cannot be scoped to a workspace")
        for k, lo, hi in (("for_seconds", 0, 604800), ("repeat_seconds", 60, 604800)):
            v = merged.get(k, 0 if k == "for_seconds" else 3600)
            if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
                raise ValidationFailed(f"{k} must be an integer between {lo} and {hi}")
        channels = merged.get("channels") or []
        if not isinstance(channels, list) or not all(isinstance(c, str) for c in channels):
            raise ValidationFailed("channels must be a list of channel ids")
        if channels:
            with self.db.connect() as conn:
                found = {r[0] for r in conn.execute(select(CH.c.id).where(CH.c.id.in_(channels), CH.c.deleted_at.is_(None))).all()}
            missing = [c for c in channels if c not in found]
            if missing:
                raise ValidationFailed(f"unknown channel(s) {missing}")
        labels = merged.get("labels") or {}
        if not isinstance(labels, dict) or len(labels) > 20:
            raise ValidationFailed("labels must be an object with at most 20 entries")
        return {"name": name, "description": str(merged.get("description") or "")[:2000], "type": typ, "severity": sev, "workspace_id": ws,
                "params": validate_params(typ, merged.get("params") if "params" in body or not existing else existing["params"]),
                "for_seconds": int(merged.get("for_seconds", 0)), "repeat_seconds": int(merged.get("repeat_seconds", 3600)), "channels": channels,
                "labels": {str(k): str(v)[:200] for k, v in labels.items()}, "enabled": bool(merged.get("enabled", True))}

    def create_rule(self, principal: Any, body: dict[str, Any]) -> dict[str, Any]:
        v = self._validate_rule(body)
        self.authorize(principal, v["workspace_id"])
        now = self.now_iso()
        row = {"id": lid("arl"), **v, "owner_id": principal.id, "etag": lid("etg")[-16:], "created_at": now, "updated_at": now}
        with self.db.begin() as conn:
            conn.execute(AR.insert().values(**row))
        self.ctx.audit.record(principal, "alert.rule_created", resource_type="alert_rule", resource_id=row["id"], workspace_id=v["workspace_id"], details={"name": v["name"], "type": v["type"]})
        return self.get_rule(row["id"])

    def get_rule(self, rule_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(AR).where(AR.c.id == rule_id, AR.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"alert rule {rule_id} not found")
        return row

    def list_rules(self, principal: Any, *, workspace_id: str | None = None) -> list[dict[str, Any]]:
        stmt = select(AR).where(AR.c.deleted_at.is_(None)).order_by(AR.c.created_at.asc())
        if workspace_id:
            stmt = stmt.where(AR.c.workspace_id == workspace_id)
        with self.db.connect() as conn:
            rows = fetch_all(conn, stmt)
        return [r for r in rows if self.can_read(principal, r["workspace_id"])]

    def update_rule(self, principal: Any, rule_id: str, body: dict[str, Any]) -> dict[str, Any]:
        rule = self.get_rule(rule_id)
        self.authorize(principal, rule["workspace_id"])
        if rule.get("builtin_key") and ({"type", "workspace_id"} & set(body)):
            raise ValidationFailed("the type and scope of a built-in rule cannot be changed")
        v = self._validate_rule(body, rule)
        with self.db.begin() as conn:
            conn.execute(update(AR).where(AR.c.id == rule_id).values(**v, updated_at=self.now_iso(), etag=lid("etg")[-16:]))
        self.ctx.audit.record(principal, "alert.rule_updated", resource_type="alert_rule", resource_id=rule_id, workspace_id=rule["workspace_id"], details={"fields": sorted(body)})
        return self.get_rule(rule_id)

    def delete_rule(self, principal: Any, rule_id: str) -> None:
        rule = self.get_rule(rule_id)
        self.authorize(principal, rule["workspace_id"])
        if rule.get("builtin_key"):
            raise BadRequest("built-in rules cannot be deleted; disable them instead", remediation="PATCH the rule with {\"enabled\": false}")
        with self.db.begin() as conn:
            conn.execute(update(AR).where(AR.c.id == rule_id).values(deleted_at=self.now_iso(), enabled=False))
        self.ctx.audit.record(principal, "alert.rule_deleted", resource_type="alert_rule", resource_id=rule_id, workspace_id=rule["workspace_id"])

    def silence_rule(self, principal: Any, rule_id: str, seconds: float | None) -> dict[str, Any]:
        rule = self.get_rule(rule_id)
        self.authorize(principal, rule["workspace_id"])
        until = to_iso(self.now() + timedelta(seconds=min(float(seconds), 90 * 86400))) if seconds else None
        with self.db.begin() as conn:
            conn.execute(update(AR).where(AR.c.id == rule_id).values(silenced_until=until, updated_at=self.now_iso()))
        self.ctx.audit.record(principal, "alert.rule_silenced", resource_type="alert_rule", resource_id=rule_id, workspace_id=rule["workspace_id"], details={"until": until})
        return self.get_rule(rule_id)

    def test_rule(self, principal: Any, rule_id: str) -> dict[str, Any]:
        """Evaluate once without touching any alert state; returns the conditions that currently hold."""
        rule = self.get_rule(rule_id)
        self.authorize(principal, rule["workspace_id"])
        conds = self.evaluators.run(rule)
        return {"rule_id": rule_id, "firing": bool(conds), "conditions": [{"key": c.key, "summary": c.summary, "value": c.value, "labels": c.labels, "details": redact_obj(c.details)} for c in conds]}

    # ------------------------------------------------------------------------------------------------------------------------------ channels
    @staticmethod
    def public_channel(row: dict[str, Any]) -> dict[str, Any]:
        cfg = dict(row.get("config") or {})
        if "url" in cfg:
            cfg["url"] = mask_url(cfg["url"])
        cfg["headers"] = redact_obj(cfg.get("headers") or {}) if "headers" in cfg else None
        if cfg.get("headers") is None:
            cfg.pop("headers", None)
        return {"id": row["id"], "workspace_id": row["workspace_id"], "name": row["name"], "type": row["type"], "config": cfg, "enabled": row["enabled"], "has_secret": bool(row.get("secret_id")),
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def _validate_channel(self, typ: str, cfg: Any) -> dict[str, Any]:
        if typ not in ("log", "webhook", "email"):
            raise ValidationFailed("channel type must be log, webhook or email")
        cfg = dict(cfg or {})
        if typ == "log":
            return {}
        if typ == "webhook":
            extra = sorted(set(cfg) - {"url", "headers"})
            if extra:
                raise ValidationFailed(f"unknown webhook channel field(s) {extra}")
            try:
                cfg["url"] = ssrf.validate_url(str(cfg.get("url") or ""), ssrf.policy_from_ctx(self.ctx))
            except OlcfError as exc:
                raise ValidationFailed(f"url rejected: {exc.message}", remediation=exc.remediation) from None
            from olcf.pipelines.webhooks import WebhookService

            cfg["headers"] = WebhookService._validate_headers(cfg.get("headers"))  # noqa: SLF001 - shared validation rules
            return cfg
        extra = sorted(set(cfg) - {"to", "subject_prefix"})
        if extra:
            raise ValidationFailed(f"unknown email channel field(s) {extra}")
        to = cfg.get("to")
        if not isinstance(to, list) or not to or len(to) > 20 or not all(isinstance(x, str) and _EMAIL.match(x) for x in to):
            raise ValidationFailed("config.to must be a list of 1-20 e-mail addresses")
        return {"to": to, "subject_prefix": str(cfg.get("subject_prefix") or "[OLCF]")[:40]}

    def create_channel(self, principal: Any, body: dict[str, Any]) -> dict[str, Any]:
        ws = body.get("workspace_id") or None
        self.authorize(principal, ws)
        name = str(body.get("name") or "").strip()
        if not name or len(name) > 128:
            raise ValidationFailed("name is required (1-128 characters)")
        typ = str(body.get("type") or "")
        cfg = self._validate_channel(typ, body.get("config"))
        now = self.now_iso()
        row = {"id": lid("chn"), "workspace_id": ws, "name": name, "type": typ, "config": cfg, "enabled": bool(body.get("enabled", True)), "owner_id": principal.id, "created_at": now, "updated_at": now}
        secret = body.get("secret")
        if secret is not None:
            if typ != "webhook" or not isinstance(secret, str) or len(secret) < 16:
                raise ValidationFailed("secret (16+ characters) is only valid for webhook channels")
            meta = self.ctx.secrets.create(f"alert-channel-{row['id']}", secret, scope_type="workspace" if ws else "org", scope_id=ws or "", created_by=principal.id, meta={"purpose": "alert-signing"})
            row["secret_id"] = meta["id"]
        with self.db.begin() as conn:
            conn.execute(CH.insert().values(**row))
        self.ctx.audit.record(principal, "alert.channel_created", resource_type="alert_channel", resource_id=row["id"], workspace_id=ws, details={"name": name, "type": typ})
        return self.get_channel(row["id"])

    def get_channel(self, channel_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(CH).where(CH.c.id == channel_id, CH.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"alert channel {channel_id} not found")
        return row

    def list_channels(self, principal: Any) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(CH).where(CH.c.deleted_at.is_(None)).order_by(CH.c.created_at.asc()))
        out = []
        for r in rows:
            try:
                self.authorize(principal, r["workspace_id"])
                out.append(r)
            except Forbidden:
                continue
        return out

    def update_channel(self, principal: Any, channel_id: str, body: dict[str, Any]) -> dict[str, Any]:
        row = self.get_channel(channel_id)
        self.authorize(principal, row["workspace_id"])
        vals: dict[str, Any] = {"updated_at": self.now_iso()}
        if "name" in body:
            vals["name"] = str(body["name"]).strip()[:128]
        if "config" in body:
            vals["config"] = self._validate_channel(row["type"], body["config"])
        if "enabled" in body:
            vals["enabled"] = bool(body["enabled"])
        with self.db.begin() as conn:
            conn.execute(update(CH).where(CH.c.id == channel_id).values(**vals))
        return self.get_channel(channel_id)

    def delete_channel(self, principal: Any, channel_id: str) -> None:
        row = self.get_channel(channel_id)
        self.authorize(principal, row["workspace_id"])
        with self.db.begin() as conn:
            conn.execute(update(CH).where(CH.c.id == channel_id).values(deleted_at=self.now_iso(), enabled=False))
        if row.get("secret_id"):
            try:
                self.ctx.secrets.delete(row["secret_id"])
            except OlcfError:
                pass

    def test_channel(self, principal: Any, channel_id: str) -> dict[str, Any]:
        ch = self.get_channel(channel_id)
        self.authorize(principal, ch["workspace_id"])
        now = self.now_iso()
        fake = {"id": "alr_test", "rule_id": "arl_test", "instance_key": "test", "severity": "info", "state": "firing", "summary": "This is a test notification from the Open Lakehouse Compute Fabric.",
                "started_at": now, "details": {"triggered_by": principal.name}, "labels": {}, "workspace_id": ch["workspace_id"]}
        payload = {"status": "test", "alert": _public_alert(fake), "rule": {"id": "arl_test", "name": "Test notification", "type": "test", "severity": "info"}}
        res = self.notifier.send(ch, payload, attempts=1)
        self.ctx.audit.record(principal, "alert.channel_tested", resource_type="alert_channel", resource_id=channel_id, workspace_id=ch["workspace_id"], outcome="success" if res["ok"] else "failure")
        return res

    # ------------------------------------------------------------------------------------------------------------------------------ evaluation
    def evaluate_all(self, *, wait: bool = False, force: bool = False) -> dict[str, Any]:
        """One evaluation pass over every rule (leader only in production). ``force`` ignores per-rule intervals. Returns a summary."""
        now = self.now()
        with self.db.connect() as conn:
            rules = fetch_all(conn, select(AR).where(AR.c.deleted_at.is_(None)))
            active_rule_ids = {r[0] for r in conn.execute(select(AL.c.rule_id).where(AL.c.state.in_(["pending", "firing"])).distinct()).all()}
        summary = {"rules": 0, "conditions": 0, "errors": 0, "fired": 0, "resolved": 0}
        known = {r["id"] for r in rules}
        for r in rules:
            if not r["enabled"]:
                if r["id"] in active_rule_ids:
                    summary["resolved"] += self._reconcile(r, [], now, quiet=True)
                continue
            interval = int((r["params"] or {}).get("interval_seconds") or 30)
            if not force and r["last_evaluated_at"] and (now - from_iso(r["last_evaluated_at"])).total_seconds() < interval:
                continue
            summary["rules"] += 1
            try:
                conds = self.evaluators.run(r)
                err = None
            except Exception as exc:  # noqa: BLE001 - a broken rule must not stop the others
                conds, err = None, f"{type(exc).__name__}: {redact_text(str(exc))[:300]}"
                summary["errors"] += 1
                log.warning("alert rule %s (%s) failed to evaluate: %s", r["id"], r["type"], err)
            with self.db.begin() as conn:
                conn.execute(update(AR).where(AR.c.id == r["id"]).values(last_evaluated_at=to_iso(now), last_error=err))
            self._metric("olcf_alert_evaluations_total", {"rule_type": r["type"], "result": "error" if err else "ok"})
            if conds is None:
                continue  # keep the current alert states: an evaluation error is not a recovery
            summary["conditions"] += len(conds)
            f, rs = self._reconcile_counts(r, conds, now)
            summary["fired"] += f
            summary["resolved"] += rs
        for rid in active_rule_ids - known:  # rules deleted while alerts were active
            with self.db.begin() as conn:
                conn.execute(update(AL).where(AL.c.rule_id == rid, AL.c.state.in_(["pending", "firing"])).values(state="resolved", resolved_at=to_iso(now), details={"resolved_reason": "rule deleted"}))
        if wait:
            self.drain()
        self.last_run = {**summary, "at": to_iso(now)}
        return summary

    def _reconcile(self, rule: dict[str, Any], conds: list[Condition], now: datetime, *, quiet: bool = False) -> int:
        return self._reconcile_counts(rule, conds, now, quiet=quiet)[1]

    def _reconcile_counts(self, rule: dict[str, Any], conds: list[Condition], now: datetime, *, quiet: bool = False) -> tuple[int, int]:
        fired = resolved = 0
        nowi = to_iso(now)
        with self.db.connect() as conn:
            active = {a["instance_key"]: a for a in fetch_all(conn, select(AL).where(AL.c.rule_id == rule["id"], AL.c.state.in_(["pending", "firing"])))}
        seen = set()
        for c in conds:
            seen.add(c.key)
            a = active.get(c.key)
            fp = hashlib.sha256(f"{rule['id']}|{c.key}".encode()).hexdigest()[:32]
            labels = {**(rule.get("labels") or {}), **c.labels}
            if a is None:
                firing_now = rule["for_seconds"] <= 0
                row = {"id": lid("alr"), "rule_id": rule["id"], "instance_key": c.key[:191], "workspace_id": c.workspace_id or rule.get("workspace_id"), "severity": rule["severity"],
                       "state": "firing" if firing_now else "pending", "summary": c.summary[:500], "value": c.value, "labels": labels, "details": redact_obj(c.details), "started_at": nowi,
                       "fired_at": nowi if firing_now else None, "last_seen_at": nowi, "notify_count": 0, "fingerprint": fp}
                with self.db.begin() as conn:
                    conn.execute(AL.insert().values(**row))
                if firing_now:
                    fired += 1
                    self._on_fire(rule, row, now)
                continue
            vals: dict[str, Any] = {"last_seen_at": nowi, "summary": c.summary[:500], "value": c.value, "labels": labels, "details": {**redact_obj(c.details), **{k: v for k, v in (a["details"] or {}).items() if k == "notifications"}},
                                    "severity": rule["severity"]}
            if a["state"] == "pending" and (now - from_iso(a["started_at"])).total_seconds() >= rule["for_seconds"]:
                vals.update(state="firing", fired_at=nowi)
                with self.db.begin() as conn:
                    conn.execute(update(AL).where(AL.c.id == a["id"]).values(**vals))
                fired += 1
                self._on_fire(rule, {**a, **vals}, now)
                continue
            with self.db.begin() as conn:
                conn.execute(update(AL).where(AL.c.id == a["id"]).values(**vals))
            if a["state"] == "firing" and not a.get("acknowledged_at"):
                last = from_iso(a["last_notified_at"]) if a.get("last_notified_at") else None
                if last is None or (now - last).total_seconds() >= rule["repeat_seconds"]:
                    self._notify(rule, {**a, **vals}, "firing", now, repeat=True)
        resolve_after = int((rule["params"] or {}).get("resolve_after_seconds") or 0)
        for key, a in active.items():
            if key in seen:
                continue
            if a["state"] == "pending" or quiet:
                with self.db.begin() as conn:
                    conn.execute(update(AL).where(AL.c.id == a["id"]).values(state="resolved", resolved_at=nowi))
                continue
            if (now - from_iso(a["last_seen_at"])).total_seconds() < resolve_after:
                continue
            with self.db.begin() as conn:
                conn.execute(update(AL).where(AL.c.id == a["id"]).values(state="resolved", resolved_at=nowi))
            resolved += 1
            self._on_resolve(rule, {**a, "state": "resolved", "resolved_at": nowi}, now)
        return fired, resolved

    def _on_fire(self, rule: dict[str, Any], alert: dict[str, Any], now: datetime) -> None:
        self._metric("olcf_alerts_fired_total", {"rule_type": rule["type"], "severity": alert["severity"]})
        self._publish("alert.firing", rule, alert)
        self._notify(rule, alert, "firing", now)

    def _on_resolve(self, rule: dict[str, Any], alert: dict[str, Any], now: datetime) -> None:
        self._publish("alert.resolved", rule, alert)
        self._notify(rule, alert, "resolved", now)

    def _publish(self, etype: str, rule: dict[str, Any], alert: dict[str, Any]) -> None:
        try:
            self.ctx.events.publish(etype, {"alert_id": alert["id"], "rule_id": rule["id"], "rule": rule["name"], "type": rule["type"], "severity": alert["severity"], "summary": alert["summary"],
                                            "instance": alert["instance_key"], "value": alert.get("value")}, workspace_id=alert.get("workspace_id"), subject=alert["id"])
        except Exception:  # noqa: BLE001
            log.exception("could not publish %s", etype)

    def _metric(self, name: str, labels: dict[str, str]) -> None:
        m = self.ctx.maybe_service("metrics")
        if m is not None:
            try:
                m.counter(name, "Alerting activity").inc(1.0, **labels)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------------------------------------------------------------------ notification
    def _channels_for(self, rule: dict[str, Any]) -> list[dict[str, Any]]:
        ids = list(rule.get("channels") or [])
        with self.db.connect() as conn:
            chans = fetch_all(conn, select(CH).where(CH.c.id.in_(ids), CH.c.deleted_at.is_(None), CH.c.enabled.is_(True))) if ids else []
        if not any(c["type"] == "log" for c in chans):
            chans.append({"id": "chn_log", "type": "log", "config": {}, "name": "log", "secret_id": None})  # every alert is at least logged
        return chans

    def _notify(self, rule: dict[str, Any], alert: dict[str, Any], status: str, now: datetime, *, repeat: bool = False) -> None:
        silenced = [x for x in (rule.get("silenced_until"), alert.get("silenced_until")) if x and x > to_iso(now)]
        nowi = to_iso(now)
        if silenced:
            self._metric("olcf_alert_notifications_total", {"channel_type": "all", "result": "silenced"})
            return
        payload = {"status": status, "alert": _public_alert({**alert, "state": "resolved" if status == "resolved" else alert.get("state", "firing")}, rule["name"], now),
                   "rule": {"id": rule["id"], "name": rule["name"], "type": rule["type"], "severity": rule["severity"]}}
        with self.db.begin() as conn:
            conn.execute(update(AL).where(AL.c.id == alert["id"]).values(last_notified_at=nowi, notify_count=AL.c.notify_count + 1))
        chans = self._channels_for(rule)

        def work() -> None:
            results = []
            for ch in chans:
                res = self.notifier.send(ch, payload)
                results.append({"channel": ch["id"], "type": ch["type"], "ok": res["ok"], "detail": str(res.get("detail"))[:200], "at": nowi, "status": status})
                self._metric("olcf_alert_notifications_total", {"channel_type": ch["type"], "result": "ok" if res["ok"] else "error"})
            try:
                with self.db.begin() as conn:
                    cur = conn.execute(select(AL.c.details).where(AL.c.id == alert["id"]).with_for_update()).scalar() or {}
                    notes = [*(cur.get("notifications") or []), *results][-20:]
                    conn.execute(update(AL).where(AL.c.id == alert["id"]).values(details={**cur, "notifications": notes}))
            except Exception:  # noqa: BLE001
                log.debug("could not record notification results", exc_info=True)

        with self._lock:
            self._pending = [f for f in self._pending if not f.done()]
            self._pending.append(self._pool.submit(work))

    def drain(self, timeout: float = 60.0) -> None:
        """Wait for queued notifications (tests, shutdown)."""
        with self._lock:
            pending = list(self._pending)
        for f in pending:
            try:
                f.result(timeout=timeout)
            except Exception:  # noqa: BLE001
                log.exception("notification task failed")

    # ------------------------------------------------------------------------------------------------------------------------------ alerts API
    def get_alert(self, principal: Any, alert_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(AL).where(AL.c.id == alert_id))
            rule = fetch_one(conn, select(AR.c.name).where(AR.c.id == row["rule_id"])) if row else None
        if not row or not self.can_read(principal, row["workspace_id"]):
            raise NotFound(f"alert {alert_id} not found")
        return _public_alert(row, rule["name"] if rule else None, self.now())

    def list_alerts(self, principal: Any, *, state: str | None = None, severity: str | None = None, rule_id: str | None = None, since: str | None = None, limit: int = 100,
                    cursor: str | None = None) -> dict[str, Any]:
        stmt = select(AL)
        if state:
            stmt = stmt.where(AL.c.state.in_([s.strip() for s in state.split(",") if s.strip()]))
        if severity:
            stmt = stmt.where(AL.c.severity == severity)
        if rule_id:
            stmt = stmt.where(AL.c.rule_id == rule_id)
        if since:
            stmt = stmt.where(AL.c.started_at >= since)
        visible = principal.visible_workspaces("events.read")
        if visible is not None:
            stmt = stmt.where(AL.c.workspace_id.in_(list(visible) or [""]))  # platform alerts (no workspace) need an organisation-level grant
        with self.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, AL, limit=limit, cursor=cursor, descending=True)
            names = {r["id"]: r["name"] for r in fetch_all(conn, select(AR.c.id, AR.c.name).where(AR.c.id.in_({r["rule_id"] for r in rows} or {""})))}
        now = self.now()
        return {"items": [_public_alert(r, names.get(r["rule_id"]), now) for r in rows], "next_cursor": nxt}

    def _manage(self, principal: Any, alert_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(AL).where(AL.c.id == alert_id))
        if not row:
            raise NotFound(f"alert {alert_id} not found")
        self.authorize(principal, row["workspace_id"])
        return row

    def acknowledge(self, principal: Any, alert_id: str) -> dict[str, Any]:
        row = self._manage(principal, alert_id)
        if row["state"] == "resolved":
            raise BadRequest("the alert is already resolved")
        if not row["acknowledged_at"]:
            with self.db.begin() as conn:
                conn.execute(update(AL).where(AL.c.id == alert_id).values(acknowledged_at=self.now_iso(), acknowledged_by=principal.id))
            self.ctx.audit.record(principal, "alert.acknowledged", resource_type="alert", resource_id=alert_id, workspace_id=row["workspace_id"])
            self.ctx.events.publish("alert.acknowledged", {"alert_id": alert_id, "by": principal.name, "summary": row["summary"]}, workspace_id=row["workspace_id"], subject=alert_id)
        return self.get_alert(principal, alert_id)

    def silence(self, principal: Any, alert_id: str, seconds: float | None) -> dict[str, Any]:
        row = self._manage(principal, alert_id)
        until = to_iso(self.now() + timedelta(seconds=min(float(seconds), 90 * 86400))) if seconds else None
        with self.db.begin() as conn:
            conn.execute(update(AL).where(AL.c.id == alert_id).values(silenced_until=until))
        self.ctx.audit.record(principal, "alert.silenced", resource_type="alert", resource_id=alert_id, workspace_id=row["workspace_id"], details={"until": until})
        return self.get_alert(principal, alert_id)

    def purge(self) -> int:
        days = int(setting(self.ctx, "alerts.history_days", 90))
        with self.db.begin() as conn:
            return conn.execute(AL.delete().where(AL.c.state == "resolved", AL.c.resolved_at < to_iso(self.now() - timedelta(days=days)))).rowcount

    def stats(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            rows = conn.execute(select(AL.c.state, AL.c.severity, func.count()).where(AL.c.state.in_(["pending", "firing"])).group_by(AL.c.state, AL.c.severity)).all()
        return {"active": [{"state": s, "severity": v, "count": int(n)} for s, v, n in rows], "last_run": self.last_run}
