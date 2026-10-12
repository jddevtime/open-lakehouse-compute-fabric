# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Triggers: schedules, webhooks, storage file arrival, pipeline dependencies, and the claim protocol that makes every firing exactly-once.

Trigger kinds (``triggers.type``)
---------------------------------
``schedule``       cron + IANA time zone bound to a pipeline / notebook / Spark job definition through a ``schedule`` *item*; catch-up policy ``skip`` | ``run_once`` | ``run_all``.
``webhook``        ``POST /v1/hooks/pipelines/{trigger_id}`` authenticated by an HMAC-SHA256 signature (timestamp bound, replay protected) or a bearer token; the payload is mapped to parameters.
``storage_event``  polls a lakehouse folder; new / changed files are batched and fired after a quiet period (debounce).
``dependency``     fires when an upstream pipeline's run reaches a state (read from the persistent event stream with a durable cursor).

Exactly-once firing
-------------------
Every firing first *claims* ``(trigger_id, fire_key)`` by inserting a ``trigger_runs`` row (unique constraint). ``fire_key`` is the ISO fire time for schedules, the
signature / delivery id for webhooks, a content hash for file batches and the event id for dependencies. The first replica to insert wins; a duplicate claim is recorded as
``duplicate`` and does nothing. The started run additionally carries the idempotency key ``trg:<trigger>:<fire_key>``, so a crash between claiming and starting is repaired by
re-claiming the stale row without creating a second run.

Schedules and time
------------------
Fire times are computed with ``croniter`` in the schedule's own time zone (DST aware) and compared in UTC. ``now`` comes from an injectable clock, which is how the tests cover
missed runs and catch-up deterministically. Creating a schedule starts it *from now*: history is never back-filled unless ``backfill_from`` is set.
"""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import logging
import re
import secrets
import threading
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import (
    BadRequest,
    Conflict,
    Forbidden,
    NotFound,
    OlcfError,
    TooManyRequests,
    Unauthenticated,
    ValidationFailed,
)
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import from_iso, to_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one
from olcf.pipelines.engine import lid
from olcf.pipelines.expressions import EvalContext, ExpressionError, analyze_value, evaluate_value
from olcf.pipelines.schema import trigger_runs as TR
from olcf.pipelines.schema import triggers as T

log = logging.getLogger("olcf.pipelines.triggers")

TARGET_TYPES = {"pipeline": "pipeline", "notebook": "notebook", "spark_job": "spark_job_definition", "spark_job_definition": "spark_job_definition"}
CATCHUP = ("skip", "run_once", "run_all")
MAX_BODY_BYTES = 1024 * 1024
MIN_INTERVAL_DEFAULT = 60
DEFAULT_GRACE = 300
_JPATH = re.compile(r"\.([A-Za-z_][\w-]*)|\[(\d+)\]|\['([^']+)'\]")


# ---------------------------------------------------------------------------------------------------------------------------------------- cron helpers
def parse_tz(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError, OSError):
        raise ValidationFailed(f"unknown time zone '{name}'", remediation="use an IANA name such as 'UTC' or 'Europe/Berlin'") from None


def validate_cron(expr: str, min_interval: float = MIN_INTERVAL_DEFAULT) -> None:
    """Raise :class:`ValidationFailed` for an invalid expression, one that never fires, or one that fires more often than ``min_interval`` seconds."""
    if not isinstance(expr, str) or not expr.strip():
        raise ValidationFailed("cron expression is required")
    fields = expr.split()
    if len(fields) not in (5, 6):
        raise ValidationFailed(f"cron expression '{expr}' must have 5 fields (minute hour day month weekday), or 6 with a trailing seconds field", remediation="example: '0 2 * * *' = every day at 02:00")
    if not croniter.is_valid(expr):
        raise ValidationFailed(f"'{expr}' is not a valid cron expression")
    base = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        it = croniter(expr, base)
        a = it.get_next(datetime)
        b = it.get_next(datetime)
    except (CroniterBadDateError, CroniterBadCronError):
        raise ValidationFailed(f"cron expression '{expr}' never fires (for example 31 February)") from None
    if (b - a).total_seconds() < min_interval:
        raise ValidationFailed(f"cron expression '{expr}' fires every {(b - a).total_seconds():g} s; the minimum interval is {min_interval:g} s",
                               remediation="lower the frequency (platform setting pipelines.min_schedule_interval_seconds)")


def fire_times(expr: str, tz: ZoneInfo, after: datetime, upto: datetime, limit: int = 1000) -> list[datetime]:
    """All fire times ``t`` with ``after < t <= upto`` (UTC, ascending, at most ``limit``)."""
    out: list[datetime] = []
    it = croniter(expr, after.astimezone(tz))
    try:
        while len(out) < limit:
            t = it.get_next(datetime).astimezone(UTC)
            if t > upto:
                break
            out.append(t)
    except CroniterBadDateError:
        pass
    return out


def next_fire_times(expr: str, tz: ZoneInfo, after: datetime, count: int = 5) -> list[datetime]:
    out: list[datetime] = []
    it = croniter(expr, after.astimezone(tz))
    try:
        for _ in range(count):
            out.append(it.get_next(datetime).astimezone(UTC))
    except CroniterBadDateError:
        pass
    return out


def previous_fire(expr: str, tz: ZoneInfo, at: datetime) -> datetime | None:
    try:
        return croniter(expr, at.astimezone(tz) + timedelta(microseconds=1)).get_prev(datetime).astimezone(UTC)
    except (CroniterBadDateError, CroniterBadCronError):
        return None


def jpath(body: Any, path: str) -> Any:
    """Minimal JSONPath: ``$.a.b[0].c`` / ``$['a b']``. Missing -> ``None``."""
    if not path.startswith("$"):
        raise BadRequest(f"payload path '{path}' must start with '$'")
    cur = body
    for m in _JPATH.finditer(path[1:]):
        key, idx, qkey = m.groups()
        if key is not None or qkey is not None:
            k = key if key is not None else qkey
            if not isinstance(cur, dict) or k not in cur:
                return None
            cur = cur[k]
        else:
            i = int(idx)
            if not isinstance(cur, list) or i >= len(cur):
                return None
            cur = cur[i]
    return cur


def _int_cfg(cfg: dict[str, Any], key: str, default: int, lo: int, hi: int) -> int:
    """Integer config value in ``[lo, hi]``; absent -> ``default``; present but invalid (including 0 when out of range) -> a precise error."""
    if cfg.get(key) is None:
        return default
    v = cfg[key]
    if isinstance(v, bool) or not isinstance(v, (int, float)) or int(v) != v or not lo <= int(v) <= hi:
        raise ValidationFailed(f"config.{key} must be an integer between {lo} and {hi} but got {v!r}")
    return int(v)


def _hash(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


# ---------------------------------------------------------------------------------------------------------------------------------------- the service
class TriggerService:
    """Owns ``triggers`` / ``trigger_runs`` and every firing path. Constructed by the pipelines module; loops are driven by :class:`TriggerLoops`."""

    def __init__(self, ctx: Any, engine: Any, clock: Any = None) -> None:
        self.ctx, self.db, self.engine = ctx, ctx.db, engine
        self._clock = clock
        self._tick_lock = threading.Lock()

    # ------------------------------------------------------------------------------------------------------------------------------ misc
    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def _setting(self, key: str, default: Any) -> Any:
        try:
            st = self.ctx.maybe_service("settings")
            if st is not None and key in st.registry:
                v = st.get(key)
                return default if v is None else v
        except Exception:  # noqa: BLE001
            pass
        return default

    def _principal(self, principal_id: str | None) -> Any:
        with self.db.connect() as conn:
            prow = fetch_one(conn, select(S.principals).where(S.principals.c.id == principal_id)) if principal_id else None
        if prow is None or not prow.get("is_active", True):
            raise Forbidden("the owner of this trigger no longer exists or is disabled", remediation="re-create the trigger as an active user or service principal")
        return self.ctx.authz.principal_from_row(prow)

    @staticmethod
    def public(row: dict[str, Any]) -> dict[str, Any]:
        out = {k: row.get(k) for k in ("id", "workspace_id", "name", "type", "target_type", "target_id", "item_id", "parameters", "enabled", "paused_until", "owner_id", "disabled_reason",
                                       "etag", "created_at", "updated_at", "last_fired_at", "last_status")}
        cfg = dict(row.get("config") or {})
        for k in ("item_etag",):
            cfg.pop(k, None)
        out["config"] = redact_obj(cfg)
        st = dict(row.get("state") or {})
        out["state"] = {k: v for k, v in st.items() if k in ("cursor", "last_check", "pending_count", "known_count", "event_seq", "initialized")}
        out["has_secret"] = bool(row.get("secret_id") or row.get("token_hash"))
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ schedule items
    def validate_schedule(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Validate a ``schedule`` item definition and return it normalized (defaults filled)."""
        if not isinstance(definition, dict):
            raise ValidationFailed("a schedule definition must be an object")
        allowed = {"cron", "timezone", "target", "enabled", "catchup", "max_catchup_runs", "start_at", "end_at", "backfill_from", "misfire_grace_seconds", "description"}
        extra = sorted(set(definition) - allowed)
        if extra:
            raise ValidationFailed(f"unknown schedule field(s) {extra}", details={"allowed": sorted(allowed)})
        d = dict(definition)
        validate_cron(d.get("cron"), float(self._setting("pipelines.min_schedule_interval_seconds", MIN_INTERVAL_DEFAULT)))
        parse_tz(d.get("timezone"))
        d["timezone"] = d.get("timezone") or "UTC"
        tgt = d.get("target")
        if not isinstance(tgt, dict) or not tgt.get("ref") or tgt.get("type") not in TARGET_TYPES:
            raise ValidationFailed("schedule.target must be {type: pipeline|notebook|spark_job_definition, ref: <id or name>, parameters: {...}}")
        extra_t = sorted(set(tgt) - {"type", "ref", "parameters"})
        if extra_t:
            raise ValidationFailed(f"unknown target field(s) {extra_t}")
        if "parameters" in tgt and not isinstance(tgt["parameters"], dict):
            raise ValidationFailed("schedule.target.parameters must be an object")
        try:
            analyze_value(tgt.get("parameters") or {})
        except ExpressionError as exc:
            raise ValidationFailed(f"invalid expression in schedule parameters: {exc.render()}") from None
        d["target"] = {"type": tgt["type"], "ref": str(tgt["ref"]), "parameters": tgt.get("parameters") or {}}
        d["catchup"] = d.get("catchup") or "skip"
        if d["catchup"] not in CATCHUP:
            raise ValidationFailed(f"catchup must be one of {', '.join(CATCHUP)}")
        mx = d.get("max_catchup_runs", 100)
        if not isinstance(mx, int) or isinstance(mx, bool) or not (1 <= mx <= 10000):
            raise ValidationFailed("max_catchup_runs must be an integer between 1 and 10000")
        d["max_catchup_runs"] = mx
        d["enabled"] = bool(d.get("enabled", True))
        g = d.get("misfire_grace_seconds", DEFAULT_GRACE)
        if not isinstance(g, (int, float)) or isinstance(g, bool) or g < 0 or g > 86400:
            raise ValidationFailed("misfire_grace_seconds must be between 0 and 86400")
        d["misfire_grace_seconds"] = g
        for k in ("start_at", "end_at", "backfill_from"):
            if d.get(k):
                try:
                    from_iso(str(d[k]))
                except ValueError:
                    raise ValidationFailed(f"{k} must be an ISO-8601 timestamp") from None
        if d.get("start_at") and d.get("end_at") and from_iso(d["end_at"]) <= from_iso(d["start_at"]):
            raise ValidationFailed("end_at must be after start_at")
        return d

    def sync_schedule_item(self, item: dict[str, Any]) -> dict[str, Any] | None:
        """Create / update the ``schedule`` trigger row backing a schedule item (idempotent)."""
        if item.get("deleted_at"):
            self.remove_schedule_item(item)
            return None
        d = self.validate_schedule(item.get("definition") or {})
        now = self.now()
        tgt = d["target"]
        target_id = ""
        try:
            target_id = self.ctx.service("items").resolve(item["workspace_id"], tgt["ref"], TARGET_TYPES[tgt["type"]])["id"]
        except OlcfError:
            target_id = ""
        with self.db.begin() as conn:
            row = fetch_one(conn, select(T).where(T.c.item_id == item["id"], T.c.type == "schedule", T.c.deleted_at.is_(None)).with_for_update())
            cfg = {**{k: d[k] for k in ("cron", "timezone", "catchup", "max_catchup_runs", "start_at", "end_at", "misfire_grace_seconds") if k in d},
                   "target": d["target"], "item_etag": item.get("etag")}
            if row is None:
                cursor = to_iso(from_iso(d["backfill_from"]) if d.get("backfill_from") else now)
                if d.get("start_at") and from_iso(d["start_at"]) > from_iso(cursor):
                    cursor = to_iso(from_iso(d["start_at"]) - timedelta(microseconds=1))
                new = {"id": lid("trg"), "workspace_id": item["workspace_id"], "name": item["name"], "type": "schedule", "target_type": TARGET_TYPES[tgt["type"]], "target_id": target_id,
                       "item_id": item["id"], "parameters": tgt["parameters"], "config": cfg, "state": {"cursor": cursor}, "enabled": d["enabled"], "owner_id": item.get("owner_id"),
                       "etag": lid("etg")[-16:], "created_at": to_iso(now), "updated_at": to_iso(now)}
                conn.execute(T.insert().values(**new))
                return new
            vals: dict[str, Any] = {"name": item["name"], "target_type": TARGET_TYPES[tgt["type"]], "target_id": target_id, "parameters": tgt["parameters"], "config": cfg,
                                    "updated_at": to_iso(now), "etag": lid("etg")[-16:]}
            if d["enabled"] != bool(row["enabled"]):
                vals["enabled"] = d["enabled"]
                if d["enabled"]:  # re-enabled: resume from now, do not replay the disabled period
                    st = dict(row["state"] or {})
                    st["cursor"] = to_iso(now)
                    vals["state"] = st
                    vals["disabled_reason"] = None
            if row["config"].get("cron") != cfg["cron"] or row["config"].get("timezone") != cfg["timezone"]:
                st = dict(vals.get("state") or row["state"] or {})
                st["cursor"] = max(st.get("cursor") or "", to_iso(now)) if not d.get("backfill_from") else st.get("cursor")
                vals["state"] = st
            conn.execute(update(T).where(T.c.id == row["id"]).values(**vals))
            return {**row, **vals}

    def remove_schedule_item(self, item: dict[str, Any]) -> None:
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.item_id == item["id"], T.c.deleted_at.is_(None)).values(deleted_at=to_iso(self.now()), enabled=False, disabled_reason="the schedule item was deleted"))

    def reconcile_schedules(self) -> int:
        """Bring ``triggers`` in line with the schedule items (covers item updates that bypass the create/delete hooks). Returns the number of changes."""
        n = 0
        with self.db.connect() as conn:
            items = fetch_all(conn, select(S.items).where(S.items.c.type == "schedule", S.items.c.deleted_at.is_(None)))
            trigs = {t["item_id"]: t for t in fetch_all(conn, select(T).where(T.c.type == "schedule", T.c.deleted_at.is_(None)))}
        live = {i["id"] for i in items}
        for it in items:
            t = trigs.get(it["id"])
            if t is None or (t["config"] or {}).get("item_etag") != it["etag"]:
                try:
                    self.sync_schedule_item(it)
                    n += 1
                except OlcfError as exc:
                    with self.db.begin() as conn:
                        if t is not None:
                            conn.execute(update(T).where(T.c.id == t["id"]).values(enabled=False, disabled_reason=f"invalid schedule: {exc.message}"[:500],
                                                                                  config={**(t["config"] or {}), "item_etag": it["etag"]}))
        for tid in trigs:
            if tid not in live:
                self.remove_schedule_item({"id": tid})
                n += 1
        return n

    # ------------------------------------------------------------------------------------------------------------------------------ queries
    def get(self, trigger_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(T).where(T.c.id == trigger_id, T.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"trigger {trigger_id} not found")
        return row

    def list(self, workspace_id: str | None = None, *, type_: str | None = None, target_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        stmt = select(T).where(T.c.deleted_at.is_(None)).order_by(T.c.created_at.desc()).limit(max(1, min(limit, 1000)))
        if workspace_id:
            stmt = stmt.where(T.c.workspace_id == workspace_id)
        if type_:
            stmt = stmt.where(T.c.type == type_)
        if target_id:
            stmt = stmt.where(T.c.target_id == target_id)
        with self.db.connect() as conn:
            return fetch_all(conn, stmt)

    def runs(self, trigger_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return fetch_all(conn, select(TR).where(TR.c.trigger_id == trigger_id).order_by(TR.c.fire_time.desc(), TR.c.id.desc()).limit(max(1, min(limit, 1000))))

    def set_enabled(self, principal: Any, trigger_id: str, enabled: bool, *, reason: str | None = None) -> dict[str, Any]:
        row = self.get(trigger_id)
        if row["type"] == "schedule" and row.get("item_id"):
            # the schedule item is the source of truth: flip it there so the two never disagree
            from olcf.core.models import ItemUpdate

            items = self.ctx.service("items")
            item = items.get(row["item_id"])
            d = dict(item.get("definition") or {})
            d["enabled"] = enabled
            items.update(principal, item["id"], ItemUpdate(definition=d, comment=f"{'enabled' if enabled else 'disabled'} via triggers API"), None)
            self.reconcile_schedules()
            return self.get(trigger_id)
        vals: dict[str, Any] = {"enabled": enabled, "updated_at": to_iso(self.now()), "disabled_reason": None if enabled else (reason or "disabled by user")}
        if enabled:
            st = dict(row["state"] or {})
            st["cursor"] = to_iso(self.now())
            vals["state"] = st
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trigger_id).values(**vals))
        return self.get(trigger_id)

    def pause(self, trigger_id: str, until: datetime | None) -> dict[str, Any]:
        self.get(trigger_id)
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trigger_id).values(paused_until=to_iso(until) if until else None, updated_at=to_iso(self.now())))
        return self.get(trigger_id)

    def preview(self, row: dict[str, Any], count: int = 5, after: datetime | None = None) -> list[dict[str, Any]]:
        cfg = row["config"] or {}
        if row["type"] != "schedule":
            raise BadRequest("only schedule triggers have fire times")
        tz = parse_tz(cfg.get("timezone"))
        out = []
        for t in next_fire_times(cfg["cron"], tz, after or self.now(), max(1, min(count, 100))):
            out.append({"utc": to_iso(t), "local": t.astimezone(tz).isoformat(), "timezone": str(tz)})
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ webhook / storage / dependency trigger CRUD
    def create(self, principal: Any, workspace_id: str, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Create a webhook / storage_event / dependency trigger. Returns ``(row, secrets)`` - ``secrets`` holds the plaintext HMAC secret or bearer token, shown once."""
        typ = body.get("type")
        if typ not in ("webhook", "storage_event", "dependency"):
            raise ValidationFailed("trigger type must be webhook, storage_event or dependency (schedules are created as schedule items)")
        name = str(body.get("name") or "").strip()
        if not name or len(name) > 128:
            raise ValidationFailed("name is required (1-128 characters)")
        tgt = body.get("target") or {}
        if tgt.get("type") not in TARGET_TYPES or not tgt.get("ref"):
            raise ValidationFailed("target must be {type: pipeline|notebook|spark_job_definition, ref: <id or name>}")
        item = self.ctx.service("items").resolve(workspace_id, str(tgt["ref"]), TARGET_TYPES[tgt["type"]])
        params = body.get("parameters") or {}
        if not isinstance(params, dict):
            raise ValidationFailed("parameters must be an object")
        try:
            analyze_value(params)
        except ExpressionError as exc:
            raise ValidationFailed(f"invalid expression in parameters: {exc.render()}") from None
        cfg = self._validate_config(typ, dict(body.get("config") or {}), workspace_id, principal)
        now = to_iso(self.now())
        row: dict[str, Any] = {"id": lid("trg"), "workspace_id": workspace_id, "name": name, "type": typ, "target_type": TARGET_TYPES[tgt["type"]], "target_id": item["id"], "parameters": params,
                               "config": cfg, "state": {}, "enabled": bool(body.get("enabled", True)), "owner_id": principal.id, "etag": lid("etg")[-16:], "created_at": now, "updated_at": now}
        shown: dict[str, Any] = {}
        if typ == "webhook":
            auth = cfg["auth"]
            if auth == "hmac":
                secret = secrets.token_urlsafe(32)
                meta = self.ctx.secrets.create(f"webhook-trigger-{row['id']}", secret, scope_type="workspace", scope_id=workspace_id, created_by=principal.id, meta={"purpose": "webhook-trigger"})
                row["secret_id"] = meta["id"]
                shown["secret"] = secret
            else:
                token = "olcf_hook_" + secrets.token_urlsafe(32)
                row["token_hash"] = hashlib.sha256(token.encode()).hexdigest()
                shown["token"] = token
            shown["url"] = f"{str(self.ctx.settings.base_url).rstrip('/')}/v1/hooks/pipelines/{row['id']}"
        if typ == "dependency":
            row["state"] = {"event_seq": self.ctx.events.head_seq()}
        try:
            with self.db.begin() as conn:
                conn.execute(T.insert().values(**row))
        except Exception:
            if row.get("secret_id"):
                try:
                    self.ctx.secrets.delete(row["secret_id"])
                except Exception:  # noqa: BLE001
                    pass
            raise
        self.ctx.audit.record(principal, "trigger.created", resource_type="trigger", resource_id=row["id"], workspace_id=workspace_id, details={"type": typ, "name": name, "target": item["id"]})
        return row, shown

    def _validate_config(self, typ: str, cfg: dict[str, Any], workspace_id: str, principal: Any) -> dict[str, Any]:
        if typ == "webhook":
            allowed = {"auth", "tolerance_seconds", "payload_mapping", "passthrough", "rate_limit_per_minute", "max_body_bytes", "signature_header", "timestamp_header"}
            extra = sorted(set(cfg) - allowed)
            if extra:
                raise ValidationFailed(f"unknown webhook config field(s) {extra}", details={"allowed": sorted(allowed)})
            cfg["auth"] = cfg.get("auth") or "hmac"
            if cfg["auth"] not in ("hmac", "bearer"):
                raise ValidationFailed("config.auth must be hmac or bearer")
            cfg["tolerance_seconds"] = _int_cfg(cfg, "tolerance_seconds", 300, 1, 3600)
            cfg["rate_limit_per_minute"] = _int_cfg(cfg, "rate_limit_per_minute", 60, 1, 6000)
            cfg["max_body_bytes"] = _int_cfg(cfg, "max_body_bytes", MAX_BODY_BYTES, 1, MAX_BODY_BYTES)
            cfg["signature_header"] = str(cfg.get("signature_header") or "X-OLCF-Signature")
            cfg["timestamp_header"] = str(cfg.get("timestamp_header") or "X-OLCF-Timestamp")
            m = cfg.get("payload_mapping") or {}
            if not isinstance(m, dict):
                raise ValidationFailed("config.payload_mapping must be an object {parameter: '$.path' | '@expression'}")
            for k, v in m.items():
                if isinstance(v, str) and v.startswith("$"):
                    continue
                try:
                    analyze_value(v)
                except ExpressionError as exc:
                    raise ValidationFailed(f"payload_mapping.{k}: {exc.render()}") from None
            cfg["payload_mapping"] = m
            cfg["passthrough"] = bool(cfg.get("passthrough", False))
        elif typ == "storage_event":
            allowed = {"lakehouse", "path", "pattern", "recursive", "poll_seconds", "debounce_seconds", "mode", "max_files_per_run", "include_existing"}
            extra = sorted(set(cfg) - allowed)
            if extra:
                raise ValidationFailed(f"unknown storage_event config field(s) {extra}", details={"allowed": sorted(allowed)})
            if not cfg.get("lakehouse") or not isinstance(cfg.get("path"), str):
                raise ValidationFailed("config.lakehouse and config.path are required")
            lh = self.ctx.service("storage").get_lakehouse(str(cfg["lakehouse"]), workspace_id=workspace_id)
            principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
            cfg["lakehouse"] = lh.id
            cfg["pattern"] = str(cfg.get("pattern") or "*")
            cfg["recursive"] = bool(cfg.get("recursive", True))
            cfg["poll_seconds"] = _int_cfg(cfg, "poll_seconds", 30, 5, 86400)
            cfg["debounce_seconds"] = _int_cfg(cfg, "debounce_seconds", 30, 0, 86400)
            cfg["mode"] = cfg.get("mode") or "batch"
            if cfg["mode"] not in ("batch", "per_file"):
                raise ValidationFailed("config.mode must be batch or per_file")
            cfg["max_files_per_run"] = _int_cfg(cfg, "max_files_per_run", 1000, 1, 10000)
            cfg["include_existing"] = bool(cfg.get("include_existing", False))
        elif typ == "dependency":
            allowed = {"upstream", "on"}
            extra = sorted(set(cfg) - allowed)
            if extra:
                raise ValidationFailed(f"unknown dependency config field(s) {extra}", details={"allowed": sorted(allowed)})
            up = cfg.get("upstream")
            if not isinstance(up, dict) or not up.get("pipeline"):
                raise ValidationFailed("config.upstream must be {pipeline: <id or name>}")
            item = self.ctx.service("items").resolve(workspace_id, str(up["pipeline"]), "pipeline")
            cfg["upstream"] = {"pipeline": item["id"]}
            on = cfg.get("on") or ["succeeded"]
            if not isinstance(on, list) or not on or any(x not in ("succeeded", "failed", "cancelled") for x in on):
                raise ValidationFailed("config.on must be a list of succeeded / failed / cancelled")
            cfg["on"] = sorted(set(on))
        return cfg

    def update(self, trigger_id: str, body: dict[str, Any], principal: Any) -> dict[str, Any]:
        row = self.get(trigger_id)
        if row["type"] == "schedule":
            raise BadRequest("schedule triggers are defined by their schedule item; update the item instead")
        vals: dict[str, Any] = {"updated_at": to_iso(self.now()), "etag": lid("etg")[-16:]}
        if "name" in body:
            vals["name"] = str(body["name"])[:128]
        if "parameters" in body:
            if not isinstance(body["parameters"], dict):
                raise ValidationFailed("parameters must be an object")
            vals["parameters"] = body["parameters"]
        if "config" in body:
            cfg = {**(row["config"] or {}), **(body["config"] or {})}
            vals["config"] = self._validate_config(row["type"], cfg, row["workspace_id"], principal)
        if "enabled" in body:
            vals["enabled"] = bool(body["enabled"])
            vals["disabled_reason"] = None if vals["enabled"] else "disabled by user"
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trigger_id).values(**vals))
        return self.get(trigger_id)

    def delete(self, trigger_id: str) -> None:
        row = self.get(trigger_id)
        if row["type"] == "schedule":
            raise BadRequest("schedule triggers are removed by deleting their schedule item")
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trigger_id).values(deleted_at=to_iso(self.now()), enabled=False))
        if row.get("secret_id"):
            try:
                self.ctx.secrets.delete(row["secret_id"])
            except Exception:  # noqa: BLE001
                pass

    def rotate_secret(self, trigger_id: str) -> dict[str, Any]:
        row = self.get(trigger_id)
        if row["type"] != "webhook":
            raise BadRequest("only webhook triggers have a secret")
        if row["config"].get("auth") == "bearer":
            token = "olcf_hook_" + secrets.token_urlsafe(32)
            with self.db.begin() as conn:
                conn.execute(update(T).where(T.c.id == trigger_id).values(token_hash=hashlib.sha256(token.encode()).hexdigest(), updated_at=to_iso(self.now())))
            return {"token": token}
        secret = secrets.token_urlsafe(32)
        self.ctx.secrets.rotate(row["secret_id"], secret)
        return {"secret": secret}

    def disable_for_target(self, target_id: str, reason: str) -> None:
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.target_id == target_id, T.c.deleted_at.is_(None), T.c.type != "schedule").values(enabled=False, disabled_reason=reason[:500]))

    # ------------------------------------------------------------------------------------------------------------------------------ firing
    def _claim(self, trg: dict[str, Any], fire_key: str, fire_time: datetime, detail: dict[str, Any] | None = None) -> str | None:
        """Insert the claim row. Returns the trigger_run id when this caller owns the firing, ``None`` for a duplicate."""
        now = to_iso(self.now())
        rid = lid("trr")
        try:
            with self.db.begin() as conn:
                conn.execute(TR.insert().values(id=rid, trigger_id=trg["id"], fire_key=fire_key[:191], fire_time=to_iso(fire_time), status="claimed", detail=detail or {}, created_at=now, updated_at=now))
            return rid
        except IntegrityError:
            pass
        # a claim that never progressed (crash between claim and start) is taken over after a minute
        stale = to_iso(self.now() - timedelta(seconds=60))
        with self.db.begin() as conn:
            row = fetch_one(conn, select(TR).where(TR.c.trigger_id == trg["id"], TR.c.fire_key == fire_key[:191]))
            if row is not None and row["status"] == "claimed" and row["run_id"] is None and row["updated_at"] < stale:
                n = conn.execute(update(TR).where(TR.c.id == row["id"], TR.c.status == "claimed", TR.c.updated_at == row["updated_at"]).values(updated_at=now)).rowcount
                if n:
                    return row["id"]
        return None

    def _record(self, trr_id: str, status: str, reason: str | None = None, run_id: str | None = None, job_id: str | None = None, detail: dict[str, Any] | None = None) -> None:
        vals: dict[str, Any] = {"status": status, "updated_at": to_iso(self.now())}
        if reason is not None:
            vals["reason"] = reason[:500]
        if run_id:
            vals["run_id"] = run_id
        if job_id:
            vals["job_id"] = job_id
        if detail is not None:
            vals["detail"] = detail
        with self.db.begin() as conn:
            conn.execute(update(TR).where(TR.c.id == trr_id).values(**vals))

    def record_skipped(self, trg: dict[str, Any], fire_key: str, fire_time: datetime, reason: str) -> None:
        trr = self._claim(trg, fire_key, fire_time)
        if trr:
            self._record(trr, "skipped", reason)
            self._count_fire(trg, "skipped")

    def _count_fire(self, trg: dict[str, Any], status: str) -> None:
        m = self.ctx.maybe_service("metrics")
        if m is not None:
            try:
                m.counter("olcf_trigger_fires_total", "Trigger firings by type and result").inc(1.0, type=trg["type"], status=status)
            except Exception:  # noqa: BLE001
                pass

    def fire(self, trg: dict[str, Any], *, fire_key: str, fire_time: datetime | None = None, body: Any = None, headers: dict[str, str] | None = None,
             extra_trigger: dict[str, Any] | None = None) -> dict[str, Any]:
        """Claim and execute one firing. Returns ``{status, run_id?, job_id?, trigger_run_id?, reason?}`` (status: started | duplicate | failed)."""
        ft = fire_time or self.now()
        trr = self._claim(trg, fire_key, ft, {"headers": redact_obj(headers or {})} if headers else {})
        if trr is None:
            self._count_fire(trg, "duplicate")
            return {"status": "duplicate", "reason": "this firing was already claimed"}
        try:
            principal = self._principal(trg.get("owner_id"))
            ws = trg["workspace_id"]
            target = self.ctx.service("items").get(trg["target_id"]) if trg.get("target_id") else self.ctx.service("items").resolve(ws, trg["config"]["target"]["ref"], trg["target_type"])
            if target["workspace_id"] != ws:
                raise Forbidden("the trigger target belongs to another workspace")
            trigger_ctx: dict[str, Any] = {"type": trg["type"], "name": trg["name"], "id": trg["id"], "scheduledTime": to_iso(ft), "startTime": to_iso(self.now()),
                                           "body": body, "headers": redact_obj(headers or {}), **(extra_trigger or {})}
            ectx = EvalContext(trigger=trigger_ctx, now=self.now, workspace={"id": ws})
            try:
                params = evaluate_value(trg.get("parameters") or {}, ectx)
            except ExpressionError as exc:
                raise ValidationFailed(f"trigger parameters could not be evaluated: {exc.render()}") from None
            idem = f"trg:{trg['id']}:{fire_key}"[:191]
            if trg["target_type"] == "pipeline":
                principal.require("pipeline.run", workspace_id=ws, item_id=target["id"])
                run = self.engine.create_run(principal, target, params, trigger_ctx, idempotency_key=idem, trigger_type=trg["type"])
                self._record(trr, "started", run_id=run["id"])
                self._finish_fire(trg, "started", ft)
                self._count_fire(trg, "started")
                self.ctx.events.publish("pipeline.trigger.fired", {"trigger_id": trg["id"], "trigger": trg["name"], "type": trg["type"], "run_id": run["id"], "pipeline_id": target["id"],
                                                                    "fire_key": fire_key}, workspace_id=ws, subject=trg["id"])
                return {"status": "started", "run_id": run["id"], "trigger_run_id": trr}
            from olcf.core.models import JobKind, JobSubmission, Origin, OriginKind

            kind = JobKind.NOTEBOOK if trg["target_type"] == "notebook" else JobKind.SPARK_JOB
            sub = JobSubmission(kind=kind, workspace_id=ws, item_id=target["id"], name=f"{trg['name']} @ {to_iso(ft)}"[:255], parameters=params,
                                origin=Origin(kind=OriginKind.SCHEDULE, details={"trigger_id": trg["id"], "trigger": trg["name"], "fire_time": to_iso(ft), "item_id": trg.get("item_id")}),
                                idempotency_key=idem, tags={"trigger": trg["id"]})
            job = self.ctx.service("jobs").submit(principal, sub)
            self._record(trr, "started", job_id=job.id)
            self._finish_fire(trg, "started", ft)
            self._count_fire(trg, "started")
            self.ctx.events.publish("pipeline.trigger.fired", {"trigger_id": trg["id"], "trigger": trg["name"], "type": trg["type"], "job_id": job.id, "item_id": target["id"], "fire_key": fire_key},
                                    workspace_id=ws, subject=trg["id"])
            return {"status": "started", "job_id": job.id, "trigger_run_id": trr}
        except Exception as exc:  # noqa: BLE001
            msg = exc.message if isinstance(exc, OlcfError) else f"{type(exc).__name__}: {redact_text(str(exc))[:300]}"
            if not isinstance(exc, OlcfError):
                log.exception("trigger %s failed to fire", trg["id"])
            self._record(trr, "failed", msg)
            self._finish_fire(trg, "failed", ft)
            self._count_fire(trg, "failed")
            try:
                self.ctx.events.publish("pipeline.trigger.failed", {"trigger_id": trg["id"], "trigger": trg["name"], "type": trg["type"], "error": msg, "fire_key": fire_key},
                                        workspace_id=trg["workspace_id"], subject=trg["id"])
            except Exception:  # noqa: BLE001
                pass
            return {"status": "failed", "reason": msg, "trigger_run_id": trr}

    def _finish_fire(self, trg: dict[str, Any], status: str, ft: datetime) -> None:
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trg["id"]).values(last_fired_at=to_iso(self.now()), last_status=status))

    def run_now(self, principal: Any, trigger_id: str) -> dict[str, Any]:
        """Fire a trigger immediately (schedules: ignores the cron; the run is marked manual so it never collides with a scheduled fire key)."""
        trg = self.get(trigger_id)
        principal.require("pipeline.run", workspace_id=trg["workspace_id"])
        owner = dict(trg)
        owner["owner_id"] = principal.id  # a manual firing runs as the person who asked for it
        return self.fire(owner, fire_key=f"manual:{lid('m')[-12:]}", fire_time=self.now(), extra_trigger={"manual": True})

    # ------------------------------------------------------------------------------------------------------------------------------ schedule loop
    def tick_schedules(self, now: datetime | None = None) -> int:
        """Fire every due schedule once (leader only). Returns the number of firings started."""
        now = now or self.now()
        started = 0
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(T).where(T.c.type == "schedule", T.c.enabled.is_(True), T.c.deleted_at.is_(None)))
        for trg in rows:
            try:
                started += self._tick_schedule(trg, now)
            except Exception:  # noqa: BLE001
                log.exception("schedule trigger %s failed", trg["id"])
        return started

    def _tick_schedule(self, trg: dict[str, Any], now: datetime) -> int:
        cfg = trg["config"] or {}
        st = dict(trg["state"] or {})
        if trg.get("paused_until"):
            if trg["paused_until"] > to_iso(now):
                return 0
            st["cursor"] = to_iso(now)  # pause over: continue from now, the paused period is not replayed
            with self.db.begin() as conn:
                conn.execute(update(T).where(T.c.id == trg["id"]).values(paused_until=None, state=st))
            return 0
        tz = parse_tz(cfg.get("timezone"))
        expr = cfg["cron"]
        cursor = from_iso(st.get("cursor") or to_iso(now))
        end_at = from_iso(cfg["end_at"]) if cfg.get("end_at") else None
        upto = min(now, end_at) if end_at else now
        if end_at and cursor >= end_at:
            return 0
        policy = cfg.get("catchup") or "skip"
        limit = int(cfg.get("max_catchup_runs") or 100)
        grace = float(cfg.get("misfire_grace_seconds", DEFAULT_GRACE))
        first = fire_times(expr, tz, cursor, upto, limit=1)
        if not first:
            return 0
        started = 0
        new_cursor = cursor
        if policy == "run_all":
            due = fire_times(expr, tz, cursor, upto, limit=limit)
            for t in due:
                res = self.fire(trg, fire_key=to_iso(t), fire_time=t)
                started += 1 if res["status"] == "started" else 0
                new_cursor = t
            more = len(due) >= limit and bool(fire_times(expr, tz, new_cursor, upto, limit=1))
            if more:
                log.info("schedule %s: catch-up continues next tick (%d fired)", trg["id"], len(due))
        else:
            probe = fire_times(expr, tz, cursor, upto, limit=1000)
            latest = probe[-1] if len(probe) < 1000 else (previous_fire(expr, tz, upto) or probe[-1])
            missed = probe[:-1] if len(probe) < 1000 else probe
            for t in missed[:200]:
                self.record_skipped(trg, to_iso(t), t, f"missed fire time (catchup={policy}: not replayed)" if policy == "skip" else "missed fire time collapsed into the latest one (catchup=run_once)")
            if len(missed) > 200 or len(probe) >= 1000:
                log.info("schedule %s: %d+ missed fire times were not recorded individually", trg["id"], len(missed))
            late = (now - latest).total_seconds()
            if policy == "skip" and late > grace:
                self.record_skipped(trg, to_iso(latest), latest, f"missed fire time ({late:.0f}s late, grace {grace:.0f}s; catchup=skip)")
            else:
                res = self.fire(trg, fire_key=to_iso(latest), fire_time=latest)
                started += 1 if res["status"] == "started" else 0
            new_cursor = latest
        if new_cursor > cursor:
            st["cursor"] = to_iso(new_cursor)
            st["last_check"] = to_iso(now)
            with self.db.begin() as conn:
                conn.execute(update(T).where(T.c.id == trg["id"]).values(state=st))
        return started

    # ------------------------------------------------------------------------------------------------------------------------------ webhook trigger endpoint
    def handle_webhook(self, trigger_id: str, headers: dict[str, str], body: bytes) -> dict[str, Any]:
        """Authenticate and process one inbound webhook call (see module docstring). Raises :class:`OlcfError` subclasses mapped to 4xx by the API layer."""
        try:
            trg = self.get(trigger_id)
        except NotFound:
            raise NotFound("webhook trigger not found") from None
        if trg["type"] != "webhook" or not trg["enabled"]:
            raise NotFound("webhook trigger not found")
        cfg = trg["config"] or {}
        h = {k.lower(): v for k, v in headers.items()}
        limit = int(cfg.get("rate_limit_per_minute") or 60)
        try:
            # a generous pre-authentication bound (CPU protection against garbage) that cannot be used to starve authenticated senders; the configured limit is applied after authentication
            self.ctx.limiter.check(f"hookpre:{trigger_id}", limit=max(600, limit * 10), window=60.0)
        except TooManyRequests:
            self._count_fire(trg, "rate_limited")
            raise
        if len(body) > int(cfg.get("max_body_bytes") or MAX_BODY_BYTES):
            raise BadRequest(f"request body is larger than {cfg.get('max_body_bytes')} bytes")
        fire_key: str
        if cfg.get("auth") == "bearer":
            auth = h.get("authorization", "")
            tok = auth[7:].strip() if auth.lower().startswith("bearer ") else h.get("x-olcf-token", "")
            if not tok or not trg.get("token_hash") or not hmac.compare_digest(hashlib.sha256(tok.encode()).hexdigest(), trg["token_hash"]):
                self._count_fire(trg, "unauthorized")
                raise Unauthenticated("invalid webhook token")
            fire_key = h.get("x-olcf-delivery") or h.get("idempotency-key") or f"b:{lid('d')[-16:]}"
        else:
            ts_raw = h.get(str(cfg.get("timestamp_header") or "X-OLCF-Timestamp").lower(), "")
            sig_raw = h.get(str(cfg.get("signature_header") or "X-OLCF-Signature").lower(), "")
            if not ts_raw or not sig_raw:
                self._count_fire(trg, "unauthorized")
                raise Unauthenticated(f"missing {cfg.get('signature_header')} / {cfg.get('timestamp_header')} header", remediation="sign '<timestamp>.<body>' with HMAC-SHA256 and send 'sha256=<hex>'")
            try:
                ts = float(ts_raw)
            except ValueError:
                raise Unauthenticated("invalid timestamp header") from None
            tol = float(cfg.get("tolerance_seconds") or 300)
            if abs(self.now().timestamp() - ts) > tol:
                self._count_fire(trg, "stale")
                raise Unauthenticated(f"timestamp is outside the allowed tolerance of {tol:g} s (replay protection)", details={"tolerance_seconds": tol})
            try:
                secret = self.ctx.secrets.resolve(trg["secret_id"])
            except OlcfError:
                raise Unauthenticated("the webhook secret is not available") from None
            secret_b = secret.encode() if isinstance(secret, str) else json.dumps(secret).encode()
            expected = hmac.new(secret_b, ts_raw.encode() + b"." + body, hashlib.sha256).hexdigest()
            presented = [p.strip().split("=", 1)[-1] for p in sig_raw.split(",") if p.strip()]
            if not any(hmac.compare_digest(expected, p) for p in presented):
                self._count_fire(trg, "unauthorized")
                raise Unauthenticated("invalid webhook signature")
            fire_key = expected   # derived from the signed content only: a replayed request cannot dodge replay protection by changing an unsigned header
        try:
            self.ctx.limiter.check(f"hook:{trigger_id}", limit=limit, window=60.0)
        except TooManyRequests:
            self._count_fire(trg, "rate_limited")
            raise
        payload: Any = None
        if body.strip():
            try:
                payload = json.loads(body)
            except ValueError:
                raise BadRequest("the request body must be JSON") from None
        params_override = self._map_payload(cfg, payload, headers)
        run_trigger = dict(trg)
        if params_override:
            run_trigger["parameters"] = {**(trg.get("parameters") or {}), **params_override}
        safe_headers = {k: v for k, v in h.items() if k in ("content-type", "user-agent", "x-olcf-delivery", "x-github-event", "x-event-type")}
        res = self.fire(run_trigger, fire_key=fire_key, fire_time=self.now(), body=payload, headers=safe_headers)
        if res["status"] == "duplicate":
            raise Conflict("this delivery was already processed (replay)", details={"fire_key": fire_key[:16] + "..."})
        if res["status"] == "failed":
            raise BadRequest(f"the trigger could not start its target: {res.get('reason')}")
        return {"status": "accepted", **{k: v for k, v in res.items() if k in ("run_id", "job_id", "trigger_run_id")}}

    def _map_payload(self, cfg: dict[str, Any], payload: Any, headers: dict[str, str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        mapping = cfg.get("payload_mapping") or {}
        ectx = EvalContext(trigger={"type": "webhook", "body": payload, "headers": {}}, now=self.now)
        for k, v in mapping.items():
            if isinstance(v, str) and v.startswith("$"):
                val = jpath(payload, v)
                if val is not None:
                    out[k] = val
            else:
                try:
                    out[k] = evaluate_value(v, ectx)
                except ExpressionError as exc:
                    raise BadRequest(f"payload_mapping.{k} could not be evaluated: {exc.render()}") from None
        if cfg.get("passthrough") and isinstance(payload, dict):
            for k, v in payload.items():
                out.setdefault(k, v)
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ storage arrival loop
    def tick_storage(self, now: datetime | None = None) -> int:
        now = now or self.now()
        fired = 0
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(T).where(T.c.type == "storage_event", T.c.enabled.is_(True), T.c.deleted_at.is_(None)))
        for trg in rows:
            try:
                fired += self._tick_storage(trg, now)
            except Exception:  # noqa: BLE001
                log.exception("storage trigger %s failed", trg["id"])
        return fired

    def _tick_storage(self, trg: dict[str, Any], now: datetime) -> int:
        cfg = trg["config"] or {}
        st = dict(trg["state"] or {})
        last = st.get("last_check")
        if last and (now - from_iso(last)).total_seconds() < float(cfg.get("poll_seconds") or 30):
            return 0
        owner = self._principal(trg.get("owner_id"))
        storage = self.ctx.service("storage")
        lh = storage.get_lakehouse(cfg["lakehouse"])
        owner.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
        base = str(cfg.get("path") or "").strip("/")
        pattern = str(cfg.get("pattern") or "*")
        prefix = base + "/" if base else ""
        files = {}
        for f in storage.list_files(lh, base, bool(cfg.get("recursive", True))):
            if f.is_dir or ".olcf-tmp/" in f.path:
                continue
            rel = f.path[len(prefix):] if f.path.startswith(prefix) else f.path
            if fnmatch.fnmatchcase(rel, pattern) or fnmatch.fnmatchcase(rel.rsplit("/", 1)[-1], pattern):
                files[f.path] = f"{f.size}:{f.modified or ''}:{f.etag or ''}"
        st["last_check"] = to_iso(now)
        known: dict[str, str] = st.get("known")  # type: ignore[assignment]
        if known is None:
            st["known"] = {} if cfg.get("include_existing") else files
            known = st["known"]
            st["initialized"] = True
            if not cfg.get("include_existing"):
                self._save_state(trg["id"], st)
                return 0
        pending: dict[str, str] = dict(st.get("pending") or {})
        changed = False
        for p, sig in files.items():
            if known.get(p) != sig:
                if pending.get(p) != sig:
                    changed = True
                pending[p] = sig
        for p in list(pending):
            if p not in files:
                pending.pop(p)
        if changed:
            st["last_change"] = to_iso(now)
        fired = 0
        quiet = (now - from_iso(st["last_change"])).total_seconds() if st.get("last_change") else 1e9
        if pending and quiet >= float(cfg.get("debounce_seconds") or 0):
            items = sorted(pending.items())[: int(cfg.get("max_files_per_run") or 1000)]
            groups = [[it] for it in items] if cfg.get("mode") == "per_file" else [items]
            for grp in groups:
                key = _hash(*[f"{p}|{s}" for p, s in grp])
                meta = [{"path": p, "signature": s} for p, s in grp]
                res = self.fire(trg, fire_key=key, fire_time=now, body={"files": meta, "count": len(meta), "lakehouse": cfg["lakehouse"], "path": base},
                                extra_trigger={"files": [m["path"] for m in meta]})
                fired += 1 if res["status"] == "started" else 0
                if res["status"] != "failed":
                    for p, s in grp:
                        known[p] = s
                        pending.pop(p, None)
        st["known"], st["pending"] = known, pending
        st["pending_count"], st["known_count"] = len(pending), len(known)
        self._save_state(trg["id"], st)
        return fired

    def _save_state(self, trigger_id: str, state: dict[str, Any]) -> None:
        with self.db.begin() as conn:
            conn.execute(update(T).where(T.c.id == trigger_id).values(state=state))

    # ------------------------------------------------------------------------------------------------------------------------------ dependency loop
    def tick_dependencies(self) -> int:
        """Fire dependency triggers for newly finished upstream runs (cursor = last processed event sequence, kept per trigger)."""
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(T).where(T.c.type == "dependency", T.c.enabled.is_(True), T.c.deleted_at.is_(None)))
        if not rows:
            return 0
        fired = 0
        for trg in rows:
            st = dict(trg["state"] or {})
            cursor = int(st.get("event_seq") or 0)
            up = (trg["config"] or {}).get("upstream", {}).get("pipeline")
            on = set((trg["config"] or {}).get("on") or ["succeeded"])
            events = self.ctx.events.list(after_seq=cursor, limit=500, type_prefix="pipeline.run.")
            last = cursor
            for e in events:
                last = max(last, int(e.get("seq") or 0))
                payload = e.get("payload") or {}
                typ = str(e["type"]).rsplit(".", 1)[-1]
                if payload.get("pipeline_id") != up or typ not in on or e.get("workspace_id") != trg["workspace_id"]:
                    continue
                res = self.fire(trg, fire_key=e["id"], fire_time=from_iso(e["ts"]), body={"upstreamRunId": e["subject"], "upstreamPipeline": payload.get("pipeline"), "state": typ,
                                                                                        "error": payload.get("error")}, extra_trigger={"upstream": e["subject"]})
                fired += 1 if res["status"] == "started" else 0
            if last != cursor:
                st["event_seq"] = last
                self._save_state(trg["id"], st)
        return fired


class TriggerLoops:
    """Leader-only background loops: schedules (every second), storage polling, dependency events and schedule-item reconciliation."""

    def __init__(self, ctx: Any, svc: TriggerService, tick_seconds: float = 1.0) -> None:
        self.ctx, self.svc, self.tick_seconds = ctx, svc, tick_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._n = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-triggers", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def tick(self) -> None:
        """One pass of every loop (callable from tests)."""
        self.svc.reconcile_schedules()
        self.svc.tick_schedules()
        self.svc.tick_storage()
        self.svc.tick_dependencies()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.ctx.leader.is_leader():
                    self._n += 1
                    self.svc.tick_schedules()
                    if self._n % 3 == 0:
                        self.svc.reconcile_schedules()
                        self.svc.tick_storage()
                        self.svc.tick_dependencies()
            except Exception:  # noqa: BLE001
                log.exception("trigger loop failed")
            self._stop.wait(self.tick_seconds)


