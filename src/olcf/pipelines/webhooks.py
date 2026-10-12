# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Outbound webhooks (spec 24): subscriptions, signed deliveries with retries, dead-letter, delivery log and a test endpoint.

Event flow
----------
``ctx.events`` (the persistent platform event stream) is the source. The *leader-only* dispatcher keeps a durable cursor (``kv`` key ``webhooks.cursor``) and for every
new event inserts one ``webhook_deliveries`` row per matching subscription. ``UNIQUE(webhook_id, event_id)`` makes the fan-out idempotent, so a failover (or two replicas
that briefly both believe they lead) never delivers an event twice. Events whose type starts with ``webhook.`` are never delivered (no feedback loops).

Event types
-----------
Patterns are matched with ``fnmatch`` against the event type: ``job.*``, ``pipeline.run.failed``, ``*``. :data:`EVENT_CATALOG` documents the families
``job.* pipeline.* item.* worker.* security.* fabric.*`` (plus ``runtime.* session.* workspace.* lakehouse.* environment.* controlplane.* cluster.* alert.*``). ``security.*`` and
``fabric.*`` events are derived from the audit log by :class:`AuditEventBridge`.

Delivery
--------
``POST <url>`` with a JSON envelope ``{id, type, created_at, workspace_id, subject, data, api_version}`` and the headers::

    X-OLCF-Event, X-OLCF-Event-Id, X-OLCF-Delivery, X-OLCF-Timestamp (unix seconds), X-OLCF-Signature: sha256=<hex>

The signature is ``HMAC-SHA256(secret, f"{timestamp}.{body}")`` - verify it with :func:`verify_signature` (receivers must also reject old timestamps). 2xx = delivered;
408/425/429/5xx and network errors are retried with exponential backoff (``webhooks.backoff_base_seconds`` * 2^(n-1), capped, +-20% jitter, ``Retry-After`` honoured);
other 4xx are permanent (``410 Gone`` additionally disables the subscription). After ``max_attempts`` the delivery becomes ``dead`` (the dead-letter queue is simply
``GET /v1/webhooks/{id}/deliveries?state=dead``; ``POST .../redeliver`` re-queues it).

Security
--------
URLs are validated at configuration time and again at connect time by the SSRF guard (private / loopback / link-local targets are refused unless allow-listed in
``network.ssrf.allow``); redirects are never followed; the signing secret lives in the secrets store and is shown once; the stored payload is redacted.
"""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import logging
import random
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
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
from olcf.pipelines.common import LeaderLoop, kv_get, kv_set, kv_set_if_absent, mask_url, setting
from olcf.pipelines.engine import lid
from olcf.pipelines.schema import webhook_deliveries as D
from olcf.pipelines.schema import webhooks as W

log = logging.getLogger("olcf.pipelines.webhooks")

CURSOR_KEY = "webhooks.cursor"
AUDIT_CURSOR_KEY = "events.audit_cursor"
API_VERSION = "1"
MAX_ENVELOPE_BYTES = 256 * 1024
MAX_RESPONSE_SNIPPET = 500
FORBIDDEN_HEADERS = {"host", "content-length", "content-type", "transfer-encoding", "connection", "upgrade", "te", "trailer", "authorization", "cookie", "proxy-authorization",
                     "set-cookie"}
_PATTERN = re.compile(r"^[a-z0-9_*.\-]{1,96}$")
_HEADER_NAME = re.compile(r"^[A-Za-z0-9\-]{1,64}$")

EVENT_CATALOG: list[dict[str, str]] = [
    {"type": "job.state_changed", "description": "A job moved to a new state: {job_id, from, to, reason}"},
    {"type": "job.spark_app_started", "description": "The Spark application of a job was registered"},
    {"type": "pipeline.run.queued", "description": "A pipeline run was created"},
    {"type": "pipeline.run.started", "description": "A pipeline run was admitted and started"},
    {"type": "pipeline.run.succeeded", "description": "A pipeline run finished successfully"},
    {"type": "pipeline.run.failed", "description": "A pipeline run failed: {run_id, pipeline, error}"},
    {"type": "pipeline.run.cancelled", "description": "A pipeline run was cancelled"},
    {"type": "pipeline.run.skipped", "description": "A pipeline run was skipped by its queue policy"},
    {"type": "pipeline.activity.failed", "description": "An activity of a run failed (after its retries)"},
    {"type": "pipeline.activity.outcome_uncertain", "description": "An activity timed out and may have done its work (operator decision needed unless it is idempotent): {run_id, activity, error, retried}"},
    {"type": "pipeline.trigger.fired", "description": "A schedule / webhook / storage / dependency trigger started a run or job"},
    {"type": "pipeline.trigger.failed", "description": "A trigger firing could not start its target"},
    {"type": "item.created", "description": "An item (notebook, pipeline, ...) was created"},
    {"type": "item.updated", "description": "An item was changed"},
    {"type": "item.deleted", "description": "An item was deleted"},
    {"type": "worker.enrolled", "description": "A worker enrolled"},
    {"type": "worker.*", "description": "Worker lifecycle: draining, offline, revoked, ..."},
    {"type": "security.login_failed", "description": "A sign-in failed (from the audit log)"},
    {"type": "security.token_created", "description": "An API token was issued"},
    {"type": "security.token_revoked", "description": "An API token was revoked"},
    {"type": "security.role_changed", "description": "A role binding changed"},
    {"type": "security.secret_changed", "description": "A secret was created, rotated or deleted"},
    {"type": "security.access_denied", "description": "An operation was rejected for security reasons"},
    {"type": "fabric.*", "description": "Microsoft Fabric integration: handshakes, submissions, diagnostics (from the audit log)"},
    {"type": "alert.firing", "description": "An alert rule started firing"},
    {"type": "alert.resolved", "description": "An alert resolved"},
    {"type": "session.*", "description": "Interactive session lifecycle"},
    {"type": "workspace.*", "description": "Workspace lifecycle"},
    {"type": "lakehouse.*", "description": "Lakehouse lifecycle"},
    {"type": "dataset.read", "description": "A job, query or pipeline read datasets: {workspace_id, process, datasets, engine, statement_hash, at}; a run submitted from a Microsoft Fabric notebook adds process.fabric {tenant_id, workspace_id, item_id, item_name, item_type, run_id} and datasets[].fabric {workspace_id, lakehouse_id}"},
    {"type": "dataset.written", "description": "A job, query or pipeline wrote datasets: {workspace_id, process, datasets, engine, mode, statement_hash, at}; a run submitted from a Microsoft Fabric notebook adds process.fabric {tenant_id, workspace_id, item_id, item_name, item_type, run_id} and datasets[].fabric {workspace_id, lakehouse_id}"},
    {"type": "dataset.statement", "description": "A SQL statement ran inside a job or session: {workspace_id, process, sql, dialect, default_lakehouse_id}; process.fabric and default_lakehouse_fabric {workspace_id, lakehouse_id} are added for Fabric-originated runs"},
    {"type": "dataset.schema_changed", "description": "The columns of a lakehouse table changed: {workspace_id, lakehouse_id, table, columns_added, columns_removed, columns_changed}"},
    {"type": "intelligence.scan.finished", "description": "A discovery scan finished: {scan_id, source_id, mode, status, version, stats}"},
    {"type": "governance.review.requested", "description": "Suggestions or change requests wait for review: {kind, count, ...}"},
    {"type": "governance.review.decided", "description": "A suggestion in the review queue was approved or rejected: {kind, decision, asset_id, ...}"},
    {"type": "governance.violation.opened", "description": "A governance policy violation was found or returned: {violation_id, policy_id, policy, rule_type, subject_type, subject_id, severity, message}"},
    {"type": "governance.violation.resolved", "description": "A governance policy violation no longer holds: {violation_id, policy_id, policy, subject_type, subject_id, severity}"},
    {"type": "governance.certification.changed", "description": "The certification of an asset, data product or term changed (also when a schema change dropped it to draft): {subject_type, subject_id, name, state, previous, reason, by}"},
    {"type": "governance.classification.changed", "description": "An approved classification was added to or removed from an asset: {asset_id, asset, classification_id, classification, status, previous}"},
    {"type": "governance.change.requested", "description": "A governance change request was opened: {change_id, type, subject_type, subject_id, subject, requester, reviewers}"},
    {"type": "governance.change.decided", "description": "A governance change request was approved or rejected: {change_id, type, decision, subject_type, subject_id, requester, decided_by}"},
    {"type": "governance.policy.evaluated", "description": "A governance policy was evaluated: {policy_id, policy, rule_type, evaluated, opened, resolved, reopened, unknown, truncated}"},
    {"type": "controlplane.leader_changed", "description": "Control-plane leadership moved"},
]


# ---------------------------------------------------------------------------------------------------------------------------------------- signing
def sign(secret: str | bytes, timestamp: str | int, body: bytes) -> str:
    """``sha256=<hex>`` over ``"<timestamp>.<body>"``."""
    key = secret.encode() if isinstance(secret, str) else secret
    return "sha256=" + hmac.new(key, str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()


def verify_signature(secret: str | bytes, timestamp: str | int, body: bytes, signature: str, *, tolerance_seconds: float | None = 300.0, now: float | None = None) -> bool:
    """Receiver-side check (used by tests and documented for integrators). Constant time; rejects timestamps older than ``tolerance_seconds``."""
    if tolerance_seconds is not None:
        try:
            if abs((now if now is not None else time.time()) - float(timestamp)) > tolerance_seconds:
                return False
        except (TypeError, ValueError):
            return False
    return hmac.compare_digest(sign(secret, timestamp, body), signature.strip())


def matches(patterns: list[str], event_type: str) -> bool:
    return any(p == "*" or fnmatch.fnmatchcase(event_type, p) for p in patterns)


def envelope_of(evt: dict[str, Any]) -> dict[str, Any]:
    data = redact_obj(evt.get("payload") or {})
    env = {"id": evt["id"], "type": evt["type"], "created_at": evt["ts"], "workspace_id": evt.get("workspace_id"), "subject": evt.get("subject"), "seq": evt.get("seq"),
           "api_version": API_VERSION, "data": data}
    size = len(json.dumps(env, default=str, separators=(",", ":")))
    if size > MAX_ENVELOPE_BYTES:
        env["data"] = {"truncated": True, "original_bytes": size, "hint": "fetch the full event from GET /v1/events"}
    return env


def canonical_body(env: dict[str, Any]) -> bytes:
    return json.dumps(env, default=str, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------------------------------------------------------------------- service
class WebhookService:
    """Subscriptions + delivery engine. ``tick()`` is one dispatcher pass (leader only in production, callable directly from tests)."""

    def __init__(self, ctx: Any, *, clock: Any = None, max_workers: int = 8, tick_seconds: float = 1.0) -> None:
        self.ctx, self.db = ctx, ctx.db
        self._clock = clock
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="olcf-whd")
        self._inflight: set[str] = set()
        self._lock = threading.Lock()
        self.loop = LeaderLoop(ctx, "webhooks", self.tick, tick_seconds, log)

    # ------------------------------------------------------------------------------------------------------------------------------ plumbing
    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def now_iso(self) -> str:
        return to_iso(self.now())

    def register_settings(self) -> None:
        st = self.ctx.maybe_service("settings")
        if st is None or "webhooks.backoff_base_seconds" in st.registry:
            return

        def pos(lo: float, hi: float) -> Any:
            def v(x: Any) -> Any:
                if isinstance(x, bool) or not isinstance(x, (int, float)) or not (lo <= x <= hi):
                    raise BadRequest(f"must be a number between {lo:g} and {hi:g}")
                return x

            return v

        st.register("webhooks.backoff_base_seconds", description="First retry delay of a failed webhook delivery (doubles each attempt)", default=5.0, validator=pos(0.01, 3600))
        st.register("webhooks.backoff_max_seconds", description="Upper bound of the delay between webhook delivery attempts", default=3600.0, validator=pos(0.01, 86400))
        st.register("webhooks.disable_after_dead", description="Disable a webhook subscription after this many consecutive dead deliveries (0 = never)", default=25, validator=pos(0, 100000))
        st.register("webhooks.retention_days", description="Days to keep finished webhook deliveries", default=14, validator=pos(1, 3650))

    def start(self) -> None:
        kv_set_if_absent(self.ctx, CURSOR_KEY, self.ctx.events.head_seq())
        self.loop.start()

    def stop(self) -> None:
        self.loop.stop()
        self._pool.shutdown(wait=True, cancel_futures=True)

    def _metric(self, kind: str, name: str, help_: str, value: float, **labels: str) -> None:
        m = self.ctx.maybe_service("metrics")
        if m is None:
            return
        try:
            if kind == "counter":
                m.counter(name, help_).inc(value, **labels)
            else:
                m.histogram(name, help_).observe(value, **labels)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------------------------------------------------------------------ authorisation / representation
    @staticmethod
    def authorize(principal: Any, workspace_id: str | None) -> None:
        """``webhook.manage`` in the workspace, or organisation-wide for subscriptions without a workspace."""
        if workspace_id:
            principal.require("webhook.manage", workspace_id=workspace_id)
        else:
            principal.require("webhook.manage")

    def _visible(self, principal: Any) -> set[str] | None:
        return principal.visible_workspaces("webhook.manage")

    @staticmethod
    def public(row: dict[str, Any]) -> dict[str, Any]:
        out = {k: row.get(k) for k in ("id", "workspace_id", "name", "event_types", "enabled", "max_attempts", "timeout_seconds", "description", "owner_id", "consecutive_failures",
                                       "last_delivery_at", "last_status", "disabled_reason", "etag", "created_at", "updated_at")}
        out["url"] = mask_url(row.get("url"))
        out["headers"] = redact_obj(row.get("headers") or {})
        out["has_secret"] = bool(row.get("secret_id"))
        return out

    @staticmethod
    def public_delivery(row: dict[str, Any], *, include_payload: bool = False) -> dict[str, Any]:
        out = {k: row.get(k) for k in ("id", "webhook_id", "event_id", "event_seq", "event_type", "workspace_id", "state", "attempts", "next_attempt_at", "last_status_code", "last_error",
                                       "last_attempt_at", "response_ms", "attempt_log", "is_test", "created_at", "delivered_at")}
        if include_payload:
            out["payload"] = row.get("payload")
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ validation
    def _validate_patterns(self, patterns: Any) -> list[str]:
        if not isinstance(patterns, list) or not patterns or len(patterns) > 50:
            raise ValidationFailed("event_types must be a non-empty list of up to 50 patterns such as 'job.*', 'pipeline.run.failed' or '*'")
        out = []
        for p in patterns:
            if not isinstance(p, str) or not _PATTERN.match(p):
                raise ValidationFailed(f"invalid event type pattern {p!r}: use lowercase letters, digits, '_', '-', '.' and '*'")
            if p.startswith("webhook."):
                raise ValidationFailed("webhook.* events are internal and cannot be subscribed to")
            out.append(p)
        return sorted(set(out))

    def _validate_url(self, url: Any) -> str:
        if not isinstance(url, str):
            raise ValidationFailed("url is required")
        try:
            return ssrf.validate_url(url, ssrf.policy_from_ctx(self.ctx))
        except OlcfError as exc:
            raise ValidationFailed(f"url rejected: {exc.message}", remediation=exc.remediation or "private networks must be allow-listed in network.ssrf.allow") from None

    @staticmethod
    def _validate_headers(headers: Any) -> dict[str, str]:
        if headers is None:
            return {}
        if not isinstance(headers, dict) or len(headers) > 20:
            raise ValidationFailed("headers must be an object with at most 20 entries")
        out = {}
        for k, v in headers.items():
            if not isinstance(k, str) or not _HEADER_NAME.match(k) or not isinstance(v, str) or len(v) > 1024 or "\n" in v or "\r" in v:
                raise ValidationFailed(f"invalid header {k!r}")
            if k.lower() in FORBIDDEN_HEADERS or k.lower().startswith("x-olcf-"):
                raise ValidationFailed(f"header '{k}' is managed by the platform or may carry credentials; headers are stored unencrypted - use the signing secret instead")
            out[k] = v
        return out

    @staticmethod
    def _limits(max_attempts: Any, timeout: Any) -> tuple[int, float]:
        try:
            if isinstance(max_attempts, bool) or isinstance(timeout, bool) or int(max_attempts) != float(max_attempts):
                raise ValueError
            attempts, seconds = int(max_attempts), float(timeout)
        except (TypeError, ValueError):
            raise ValidationFailed("max_attempts must be an integer 1-20 and timeout_seconds a number 1-60") from None
        if not (1 <= attempts <= 20) or not (1 <= seconds <= 60):
            raise ValidationFailed("max_attempts must be 1-20 and timeout_seconds 1-60")
        return attempts, seconds

    # ------------------------------------------------------------------------------------------------------------------------------ CRUD
    def create(self, principal: Any, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        ws = body.get("workspace_id") or None
        if ws:
            ws = self.ctx.service("workspaces").resolve(str(ws))["id"]
        self.authorize(principal, ws)
        name = str(body.get("name") or "").strip()
        if not name or len(name) > 128:
            raise ValidationFailed("name is required (1-128 characters)")
        url = self._validate_url(body.get("url"))
        patterns = self._validate_patterns(body.get("event_types"))
        max_attempts, timeout = self._limits(body.get("max_attempts", 8), body.get("timeout_seconds", 10))
        secret = body.get("secret")
        if secret is not None and (not isinstance(secret, str) or not (16 <= len(secret) <= 256)):
            raise ValidationFailed("secret must be a string of 16-256 characters (omit it to have one generated)")
        generated = secret is None
        secret = secret or "whsec_" + secrets.token_urlsafe(32)
        now = self.now_iso()
        row = {"id": lid("hok"), "workspace_id": ws, "name": name, "url": url, "event_types": patterns, "headers": self._validate_headers(body.get("headers")),
               "enabled": bool(body.get("enabled", True)), "max_attempts": max_attempts, "timeout_seconds": timeout, "description": str(body.get("description") or "")[:2000],
               "owner_id": principal.id, "consecutive_failures": 0, "etag": lid("etg")[-16:], "created_at": now, "updated_at": now}
        meta = self.ctx.secrets.create(f"webhook-{row['id']}", secret, scope_type="workspace" if ws else "org", scope_id=ws or "", created_by=principal.id, meta={"purpose": "webhook-signing"})
        row["secret_id"] = meta["id"]
        try:
            with self.db.begin() as conn:
                conn.execute(W.insert().values(**row))
        except Exception:
            self.ctx.secrets.delete(meta["id"])
            raise
        self.ctx.audit.record(principal, "webhook.created", resource_type="webhook", resource_id=row["id"], workspace_id=ws, details={"name": name, "events": patterns, "url": mask_url(url)})
        return row, ({"secret": secret} if generated else {})

    def get(self, webhook_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(W).where(W.c.id == webhook_id, W.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"webhook {webhook_id} not found")
        return row

    def get_authorized(self, principal: Any, webhook_id: str) -> dict[str, Any]:
        row = self.get(webhook_id)
        try:
            self.authorize(principal, row["workspace_id"])
        except Forbidden:
            raise NotFound(f"webhook {webhook_id} not found") from None  # do not reveal other tenants' subscriptions
        return row

    def list(self, principal: Any, workspace_id: str | None = None) -> list[dict[str, Any]]:
        visible = self._visible(principal)
        stmt = select(W).where(W.c.deleted_at.is_(None)).order_by(W.c.created_at.desc())
        if workspace_id:
            principal.require("webhook.manage", workspace_id=workspace_id)
            stmt = stmt.where(W.c.workspace_id == workspace_id)
        elif visible is not None:
            stmt = stmt.where(W.c.workspace_id.in_(list(visible) or [""]))
        with self.db.connect() as conn:
            return fetch_all(conn, stmt)

    def update(self, principal: Any, webhook_id: str, body: dict[str, Any]) -> dict[str, Any]:
        row = self.get_authorized(principal, webhook_id)
        vals: dict[str, Any] = {"updated_at": self.now_iso(), "etag": lid("etg")[-16:]}
        if "name" in body:
            nm = str(body["name"]).strip()
            if not nm or len(nm) > 128:
                raise ValidationFailed("name must be 1-128 characters")
            vals["name"] = nm
        if "url" in body:
            vals["url"] = self._validate_url(body["url"])
        if "event_types" in body:
            vals["event_types"] = self._validate_patterns(body["event_types"])
        if "headers" in body:
            vals["headers"] = self._validate_headers(body["headers"])
        if "description" in body:
            vals["description"] = str(body["description"] or "")[:2000]
        if "max_attempts" in body or "timeout_seconds" in body:
            attempts, seconds = self._limits(body.get("max_attempts", row["max_attempts"]), body.get("timeout_seconds", row["timeout_seconds"]))
            if "max_attempts" in body:
                vals["max_attempts"] = attempts
            if "timeout_seconds" in body:
                vals["timeout_seconds"] = seconds
        if "enabled" in body:
            vals["enabled"] = bool(body["enabled"])
            if vals["enabled"]:
                vals.update(disabled_reason=None, consecutive_failures=0)
        with self.db.begin() as conn:
            conn.execute(update(W).where(W.c.id == webhook_id).values(**vals))
        self.ctx.audit.record(principal, "webhook.updated", resource_type="webhook", resource_id=webhook_id, workspace_id=row["workspace_id"], details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag"))})
        return self.get(webhook_id)

    def delete(self, principal: Any, webhook_id: str) -> None:
        row = self.get_authorized(principal, webhook_id)
        with self.db.begin() as conn:
            conn.execute(update(W).where(W.c.id == webhook_id).values(deleted_at=self.now_iso(), enabled=False))
            conn.execute(D.delete().where(D.c.webhook_id == webhook_id, D.c.state.in_(["pending", "retrying"])))
        if row.get("secret_id"):
            try:
                self.ctx.secrets.delete(row["secret_id"])
            except OlcfError:
                pass
        self.ctx.audit.record(principal, "webhook.deleted", resource_type="webhook", resource_id=webhook_id, workspace_id=row["workspace_id"])

    def rotate_secret(self, principal: Any, webhook_id: str, new_secret: str | None = None) -> dict[str, Any]:
        row = self.get_authorized(principal, webhook_id)
        secret = new_secret or "whsec_" + secrets.token_urlsafe(32)
        if len(secret) < 16:
            raise ValidationFailed("secret must be at least 16 characters")
        self.ctx.secrets.rotate(row["secret_id"], secret)
        self.ctx.audit.record(principal, "webhook.secret_rotated", resource_type="webhook", resource_id=webhook_id, workspace_id=row["workspace_id"])
        return {"secret": secret}

    # ------------------------------------------------------------------------------------------------------------------------------ deliveries (read side)
    def deliveries(self, principal: Any, webhook_id: str, *, state: str | None = None, event_type: str | None = None, limit: int = 50, cursor: str | None = None) -> dict[str, Any]:
        self.get_authorized(principal, webhook_id)
        stmt = select(D).where(D.c.webhook_id == webhook_id)
        if state:
            stmt = stmt.where(D.c.state.in_([s.strip() for s in state.split(",") if s.strip()]))
        if event_type:
            stmt = stmt.where(D.c.event_type.like(event_type.replace("*", "%")))
        with self.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, D, limit=limit, cursor=cursor, descending=True)
        return {"items": [self.public_delivery(r) for r in rows], "next_cursor": nxt}

    def get_delivery(self, principal: Any, delivery_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(D).where(D.c.id == delivery_id))
        if not row:
            raise NotFound(f"delivery {delivery_id} not found")
        self.get_authorized(principal, row["webhook_id"])
        return row

    def redeliver(self, principal: Any, delivery_id: str) -> dict[str, Any]:
        row = self.get_delivery(principal, delivery_id)
        if row["state"] in ("pending", "retrying", "delivering"):
            raise BadRequest(f"delivery {delivery_id} is still {row['state']}")
        with self.db.begin() as conn:
            conn.execute(update(D).where(D.c.id == delivery_id).values(state="pending", attempts=0, next_attempt_at=self.now_iso(), last_error=None, delivered_at=None))
        self.ctx.audit.record(principal, "webhook.redeliver", resource_type="webhook_delivery", resource_id=delivery_id, workspace_id=row["workspace_id"])
        return self.get_delivery(principal, delivery_id)

    # ------------------------------------------------------------------------------------------------------------------------------ test delivery
    def test(self, principal: Any, webhook_id: str) -> dict[str, Any]:
        """Send one synthetic ``webhook.test`` event synchronously (no retries) and return the recorded delivery."""
        hook = self.get_authorized(principal, webhook_id)
        evt = {"id": lid("evt"), "ts": self.now_iso(), "type": "webhook.test", "workspace_id": hook["workspace_id"], "subject": hook["id"], "seq": None,
               "payload": {"message": "This is a test event from the Open Lakehouse Compute Fabric.", "webhook": hook["name"], "triggered_by": principal.name}}
        drow = {"id": lid("whd"), "webhook_id": hook["id"], "event_id": evt["id"], "event_seq": None, "event_type": "webhook.test", "workspace_id": hook["workspace_id"],
                "payload": envelope_of(evt), "state": "delivering", "attempts": 0, "attempt_log": [], "is_test": True, "created_at": self.now_iso()}
        with self.db.begin() as conn:
            conn.execute(D.insert().values(**drow))
        self.ctx.audit.record(principal, "webhook.test", resource_type="webhook", resource_id=webhook_id, workspace_id=hook["workspace_id"])
        self._attempt(drow["id"], hook, test=True)
        return self.get_delivery(principal, drow["id"])

    # ------------------------------------------------------------------------------------------------------------------------------ dispatcher
    def tick(self) -> dict[str, int]:
        """One pass: fan new events out, recover stuck deliveries, send what is due. Returns counters (for tests / diagnostics)."""
        n_new = self.fanout()
        self._recover_stuck()
        n_sent = self.deliver_due()
        return {"enqueued": n_new, "attempted": n_sent}

    def fanout(self, limit: int = 500) -> int:
        cursor = kv_get(self.ctx, CURSOR_KEY)
        if cursor is None:
            kv_set_if_absent(self.ctx, CURSOR_KEY, self.ctx.events.head_seq())
            cursor = kv_get(self.ctx, CURSOR_KEY, 0)
        events = self.ctx.events.list(after_seq=int(cursor), limit=limit)
        if not events:
            return 0
        with self.db.connect() as conn:
            hooks = fetch_all(conn, select(W).where(W.c.enabled.is_(True), W.c.deleted_at.is_(None)))
        n = 0
        for e in events:
            if str(e["type"]).startswith("webhook."):
                continue
            for h in hooks:
                if h["workspace_id"] and h["workspace_id"] != e.get("workspace_id"):
                    continue
                if e["ts"] < h["created_at"] or not matches(h["event_types"] or [], e["type"]):
                    continue
                row = {"id": lid("whd"), "webhook_id": h["id"], "event_id": e["id"], "event_seq": e.get("seq"), "event_type": e["type"], "workspace_id": e.get("workspace_id"),
                       "payload": envelope_of(e), "state": "pending", "attempts": 0, "next_attempt_at": self.now_iso(), "attempt_log": [], "is_test": False, "created_at": self.now_iso()}
                try:
                    with self.db.begin() as conn:
                        conn.execute(D.insert().values(**row))
                    n += 1
                except IntegrityError:
                    pass  # already enqueued by a previous leader
        kv_set(self.ctx, CURSOR_KEY, max(int(e.get("seq") or 0) for e in events))
        return n

    def _recover_stuck(self) -> None:
        """A delivery left ``delivering`` by a crashed leader is retried (the attempt was already counted)."""
        cutoff = to_iso(self.now() - timedelta(seconds=90))
        with self.db.begin() as conn:
            with self._lock:
                busy = list(self._inflight)
            stmt = update(D).where(D.c.state == "delivering", D.c.last_attempt_at < cutoff)
            if busy:
                stmt = stmt.where(D.c.id.not_in(busy))
            conn.execute(stmt.values(state="retrying", next_attempt_at=self.now_iso(), last_error="the dispatcher stopped before this attempt finished"))

    def deliver_due(self, limit: int = 100) -> int:
        now = self.now_iso()
        with self.db.connect() as conn:
            due = fetch_all(conn, select(D.c.id, D.c.webhook_id, D.c.next_attempt_at, D.c.state).where(D.c.state.in_(["pending", "retrying"]), D.c.next_attempt_at <= now)
                            .order_by(D.c.next_attempt_at.asc()).limit(limit))
            hooks = {h["id"]: h for h in fetch_all(conn, select(W).where(W.c.id.in_({d["webhook_id"] for d in due} or {""}), W.c.deleted_at.is_(None)))}
        futures = []
        for d in due:
            hook = hooks.get(d["webhook_id"])
            with self._lock:
                if d["id"] in self._inflight:
                    continue
            if hook is None or not hook["enabled"]:
                with self.db.begin() as conn:
                    conn.execute(update(D).where(D.c.id == d["id"], D.c.state == d["state"]).values(state="dead", last_error="the webhook was deleted or disabled"))
                continue
            claimed = self._claim(d)
            if not claimed:
                continue
            with self._lock:
                self._inflight.add(d["id"])
            futures.append(self._pool.submit(self._attempt_guarded, d["id"], hook))
        for f in futures:
            try:
                f.result(timeout=120)
            except Exception:  # noqa: BLE001
                log.exception("webhook delivery task failed")
        return len(futures)

    def _claim(self, d: dict[str, Any]) -> bool:
        with self.db.begin() as conn:
            n = conn.execute(update(D).where(D.c.id == d["id"], D.c.state == d["state"], D.c.next_attempt_at == d["next_attempt_at"])
                             .values(state="delivering", attempts=D.c.attempts + 1, last_attempt_at=self.now_iso())).rowcount
        return bool(n)

    def _attempt_guarded(self, delivery_id: str, hook: dict[str, Any]) -> None:
        try:
            self._attempt(delivery_id, hook)
        finally:
            with self._lock:
                self._inflight.discard(delivery_id)

    def _backoff(self, attempt: int, retry_after: float | None) -> float:
        base = float(setting(self.ctx, "webhooks.backoff_base_seconds", 5.0))
        cap = float(setting(self.ctx, "webhooks.backoff_max_seconds", 3600.0))
        delay = min(cap, base * (2 ** max(0, attempt - 1)))
        delay *= random.uniform(0.8, 1.2)  # noqa: S311 - jitter, not security relevant
        if retry_after is not None:
            delay = max(delay, min(retry_after, cap))
        return min(delay, cap)

    def _attempt(self, delivery_id: str, hook: dict[str, Any], *, test: bool = False) -> None:
        """Perform one HTTP attempt and record the outcome (state transition, attempt log, subscription health)."""
        with self.db.connect() as conn:
            d = fetch_one(conn, select(D).where(D.c.id == delivery_id))
            live = fetch_one(conn, select(W.c.enabled, W.c.deleted_at).where(W.c.id == hook["id"]))
        if d is None:
            return
        if not test and (live is None or live["deleted_at"] or not live["enabled"]):
            # disabled (or deleted) after this delivery was picked up - for example by the failures of its siblings that ran in the same pass: no request, no retry,
            # and the failure counter of the subscription is left alone
            with self.db.begin() as conn:
                conn.execute(update(D).where(D.c.id == delivery_id, D.c.state == "delivering").values(state="dead", next_attempt_at=None, last_error="the webhook was deleted or disabled"))
            return
        attempt = int(d["attempts"]) + (1 if test else 0)
        body = canonical_body(d["payload"])
        ts = str(int(self.now().timestamp()))
        status: int | None = None
        error: str | None = None
        retryable = True
        retry_after: float | None = None
        snippet = ""
        t0 = time.perf_counter()
        try:
            secret = self.ctx.secrets.resolve(hook["secret_id"]) if hook.get("secret_id") else None
        except OlcfError:
            secret, error, retryable = None, "the signing secret is not available", False
        if error is None:
            headers = {"Content-Type": "application/json", "User-Agent": "olcf-webhooks/1", "X-OLCF-Event": d["event_type"], "X-OLCF-Event-Id": d["event_id"], "X-OLCF-Delivery": d["id"],
                       "X-OLCF-Attempt": str(attempt), "X-OLCF-Timestamp": ts, **(hook.get("headers") or {})}
            if secret is not None:
                headers["X-OLCF-Signature"] = sign(secret if isinstance(secret, str) else json.dumps(secret), ts, body)
            try:
                policy = ssrf.policy_from_ctx(self.ctx)
                ssrf.validate_url(hook["url"], policy)
                with ssrf.guarded_client(policy, timeout=float(hook["timeout_seconds"]), follow_redirects=False) as client, client.stream("POST", hook["url"], content=body, headers=headers) as resp:
                    status = resp.status_code
                    raw, _ = ssrf.read_capped(resp, 4096)
                    snippet = redact_text(raw.decode("utf-8", "replace"))[:MAX_RESPONSE_SNIPPET]
                    ra = resp.headers.get("retry-after")
                    if ra:
                        try:
                            retry_after = float(ra)
                        except ValueError:
                            retry_after = None
            except OlcfError as exc:  # SSRF guard (configuration changed, DNS rebinding, ...)
                error, retryable = f"blocked: {exc.message}", False
            except httpx.TimeoutException:
                error = f"timed out after {hook['timeout_seconds']:g}s"
            except httpx.HTTPError as exc:
                error = f"{type(exc).__name__}: {redact_text(str(exc))[:200]}"
        ms = int((time.perf_counter() - t0) * 1000)
        ok = status is not None and 200 <= status < 300
        if status is not None and not ok:
            error = f"HTTP {status}" + (f": {snippet}" if snippet else "")
            retryable = status in (408, 425, 429) or status >= 500
        now = self.now_iso()
        entry = {"attempt": attempt, "at": now, "status": status, "error": error, "ms": ms}
        gone = status == 410
        auto_disabled = False
        max_attempts = int(hook["max_attempts"])
        if ok:
            new_state, nxt = "succeeded", None
        elif test or not retryable or attempt >= max_attempts:
            new_state, nxt = "dead", None
        else:
            new_state = "retrying"
            nxt = to_iso(self.now() + timedelta(seconds=self._backoff(attempt, retry_after)))
        with self.db.begin() as conn:
            cur = conn.execute(select(D.c.attempt_log).where(D.c.id == delivery_id).with_for_update()).scalar() or []
            log_ = [*cur, entry][-20:]
            conn.execute(update(D).where(D.c.id == delivery_id).values(
                state=new_state, next_attempt_at=nxt, last_status_code=status, last_error=(error[:1000] if error else None), last_attempt_at=now, response_ms=ms, attempt_log=log_,
                delivered_at=now if ok else None, **({"attempts": attempt} if test else {})))
            if not d["is_test"]:
                if ok:
                    conn.execute(update(W).where(W.c.id == hook["id"]).values(consecutive_failures=0, last_delivery_at=now, last_status="succeeded"))
                elif new_state == "dead":
                    # counted only while the subscription is enabled, and the auto-disable happens in the same transaction: concurrent attempts of one pass cannot push
                    # the counter past the limit (the update of the next one no longer matches) and a disabled webhook's counter stays where it was
                    counted = conn.execute(update(W).where(W.c.id == hook["id"], W.c.enabled.is_(True)).values(
                        consecutive_failures=W.c.consecutive_failures + 1, last_delivery_at=now, last_status="dead")).rowcount
                    limit = int(setting(self.ctx, "webhooks.disable_after_dead", 25))
                    if counted and limit > 0 and not gone:
                        fails = conn.execute(select(W.c.consecutive_failures).where(W.c.id == hook["id"])).scalar() or 0
                        if fails >= limit:
                            auto_disabled = self._disable_in(conn, hook["id"], f"{fails} consecutive deliveries failed permanently")
                else:
                    conn.execute(update(W).where(W.c.id == hook["id"], W.c.enabled.is_(True)).values(last_delivery_at=now, last_status="retrying"))
        self._metric("counter", "olcf_webhook_deliveries_total", "Webhook delivery attempts by outcome", 1.0, state=new_state if new_state != "retrying" else "retry")
        self._metric("hist", "olcf_webhook_delivery_seconds", "Webhook delivery attempt duration", ms / 1000.0)
        if gone and not d["is_test"]:
            self._disable(hook["id"], "the endpoint answered 410 Gone")
        elif auto_disabled:
            log.warning("webhook %s disabled after consecutive permanent failures", hook["id"])

    def _disable_in(self, conn: Any, webhook_id: str, reason: str) -> bool:
        """Disable the subscription inside ``conn`` (only when it is still enabled) and dead-letter its queued deliveries, so a disabled webhook produces neither
        requests nor retry noise. Returns whether this call did the disabling."""
        done = conn.execute(update(W).where(W.c.id == webhook_id, W.c.enabled.is_(True)).values(enabled=False, disabled_reason=reason, updated_at=self.now_iso())).rowcount
        if done:
            conn.execute(update(D).where(D.c.webhook_id == webhook_id, D.c.state.in_(["pending", "retrying"])).values(
                state="dead", next_attempt_at=None, last_error="the webhook was deleted or disabled"))
        return bool(done)

    def _disable(self, webhook_id: str, reason: str) -> None:
        with self.db.begin() as conn:
            done = self._disable_in(conn, webhook_id, reason)
        if done:
            log.warning("webhook %s disabled: %s", webhook_id, reason)

    # ------------------------------------------------------------------------------------------------------------------------------ housekeeping / stats
    def purge(self) -> int:
        days = int(setting(self.ctx, "webhooks.retention_days", 14))
        cutoff = to_iso(self.now() - timedelta(days=days))
        with self.db.begin() as conn:
            return conn.execute(D.delete().where(D.c.state.in_(["succeeded", "dead"]), D.c.created_at < cutoff)).rowcount

    def stats(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            rows = conn.execute(select(D.c.state, func.count()).group_by(D.c.state)).all()
            oldest = conn.execute(select(func.min(D.c.next_attempt_at)).where(D.c.state.in_(["pending", "retrying"]))).scalar()
        lag = None
        if oldest:
            lag = max(0.0, (self.now() - from_iso(oldest)).total_seconds())
        return {"deliveries_by_state": {r[0]: int(r[1]) for r in rows}, "oldest_due_lag_seconds": lag}


# ---------------------------------------------------------------------------------------------------------------------------------------- audit -> events
_AUDIT_MAP = {
    "auth.login_failed": "security.login_failed", "auth.login": "security.login", "auth.token_created": "security.token_created", "auth.token_revoked": "security.token_revoked",
    "identity.binding_changed": "security.role_changed", "identity.password_changed": "security.password_changed", "identity.user_created": "security.user_created",
    "identity.user_updated": "security.user_updated", "secret.created": "security.secret_changed", "secret.rotated": "security.secret_changed", "secret.deleted": "security.secret_changed",
    "worker.enrollment_token_created": "security.enrollment_token_created", "worker.identity_mismatch": "security.worker_identity_mismatch", "audit.exported": "security.audit_exported",
    "config.changed": "security.config_changed", "data.destructive": "security.destructive_operation", "webhook.created": "security.webhook_changed",
    "webhook.updated": "security.webhook_changed", "webhook.deleted": "security.webhook_changed", "webhook.secret_rotated": "security.webhook_changed",
}


class AuditEventBridge:
    """Leader-only: turns security-relevant audit records into ``security.*`` events and ``fabric.*`` audit records into ``fabric.*`` events so subscribers see one stream.

    A durable cursor (``events.audit_cursor``) makes this exactly-once across restarts; the audit log stays the system of record.
    """

    def __init__(self, ctx: Any, interval: float = 2.0) -> None:
        self.ctx = ctx
        self.loop = LeaderLoop(ctx, "audit-events", self.tick, interval, log)

    def start(self) -> None:
        with self.ctx.db.connect() as conn:
            head = conn.execute(select(func.max(S.audit_log.c.seq))).scalar() or 0
        kv_set_if_absent(self.ctx, AUDIT_CURSOR_KEY, int(head))
        self.loop.start()

    def stop(self) -> None:
        self.loop.stop()

    def tick(self, limit: int = 500) -> int:
        cursor = int(kv_get(self.ctx, AUDIT_CURSOR_KEY, 0) or 0)
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(S.audit_log).where(S.audit_log.c.seq > cursor).order_by(S.audit_log.c.seq.asc()).limit(limit))
        if not rows:
            return 0
        n = 0
        for r in rows:
            etype = self.event_type_for(r)
            if etype:
                payload = {"action": r["action"], "outcome": r["outcome"], "actor": r.get("actor_name"), "actor_kind": r.get("actor_kind"), "resource_type": r.get("resource_type"),
                           "resource_id": r.get("resource_id"), "ip": r.get("ip"), "audit_id": r["id"], "details": redact_obj(r.get("details") or {}), "source": "audit"}
                self.ctx.events.publish(etype, payload, workspace_id=r.get("workspace_id"), subject=r.get("resource_id"))
                n += 1
        kv_set(self.ctx, AUDIT_CURSOR_KEY, rows[-1]["seq"])
        return n

    @staticmethod
    def event_type_for(r: dict[str, Any]) -> str | None:
        action = r["action"]
        if action in _AUDIT_MAP:
            return _AUDIT_MAP[action]
        if action.startswith("fabric."):
            return action + (".failed" if r.get("outcome") not in (None, "success") and not action.endswith("rejected") else "")
        if r.get("outcome") in ("denied", "forbidden"):
            return "security.access_denied"
        return None


__all__ = ["EVENT_CATALOG", "AuditEventBridge", "WebhookService", "canonical_body", "envelope_of", "matches", "sign", "verify_signature"]
