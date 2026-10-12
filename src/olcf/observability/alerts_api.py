# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of alerting (spec 28), mounted at ``/v1/alerts``.

``GET /rule-types``                        parameter documentation of every rule type
``GET|POST /rules``, ``GET|PATCH|DELETE /rules/{id}``, ``POST /rules/{id}/test|silence``
``GET|POST /channels``, ``PATCH|DELETE /channels/{id}``, ``POST /channels/{id}/test``
``GET /`` (history and active alerts, filter by ``state``, ``severity``, ``rule_id``, ``since``), ``GET /{id}``, ``POST /{id}/ack``, ``POST /{id}/silence``
``POST /evaluate``                         run the evaluation now (leader or not; used by operators and tests)

Authorisation: reading needs ``events.read`` (platform alerts: organisation level; workspace alerts: that workspace); every mutation needs ``alert.manage`` on the rule's
workspace, or at organisation level for platform rules and channels.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response
from pydantic import Field, StrictFloat, StrictInt

from olcf.api.deps import Authed, Ctx, IfMatch, check_if_match, etag_header
from olcf.core.errors import Unavailable
from olcf.core.models import OpenBody, dump
from olcf.observability.alerts import RULE_TYPES, SEVERITIES

router = APIRouter(tags=["alerts"])


class RuleBody(OpenBody):
    """Create / update an alert rule. ``type`` and ``name`` are required on create; the service validates the rest (and ``params`` per rule type, see ``GET /rule-types``)."""

    name: str | None = Field(None, description="1-128 characters")
    type: str | None = Field(None, description="rule type (GET /v1/alerts/rule-types); cannot be changed after creation")
    description: str | None = None
    severity: str | None = Field(None, description="info | warning | critical (default warning)")
    params: dict[str, Any] | None = Field(None, description="parameters of the rule type")
    for_seconds: StrictInt | None = Field(None, description="the condition must hold this long before the alert fires (0-604800)")
    repeat_seconds: StrictInt | None = Field(None, description="re-notify interval (60-604800)")
    channels: list[str] | None = Field(None, description="ids of the notification channels")
    labels: dict[str, Any] | None = Field(None, description="up to 20 labels")
    enabled: bool | None = None
    workspace_id: str | None = Field(None, description="scope of a workspace rule; omit for platform rules")


class ChannelBody(OpenBody):
    """Create / update a notification channel (``type`` is fixed at creation)."""

    name: str | None = None
    type: str | None = Field(None, description="log | webhook | email")
    config: dict[str, Any] | None = Field(None, description="webhook: {url, headers?}; email: {to: [..], subject_prefix?}")
    workspace_id: str | None = None
    secret: str | None = Field(None, description="write-only secret (webhook signing / authorisation)")
    enabled: bool | None = None


class SilenceBody(OpenBody):
    """Silence notifications; ``minutes`` 0 lifts the silence (default 60, at most 129600)."""

    minutes: StrictInt | StrictFloat | None = None


def _svc(ctx: Any) -> Any:
    svc = ctx.maybe_service("alerts")
    if svc is None:
        raise Unavailable("the alerting service is not running")
    return svc


def _rule_out(svc: Any, row: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "workspace_id", "name", "description", "type", "severity", "params", "for_seconds", "repeat_seconds", "channels", "labels", "enabled", "builtin_key", "silenced_until",
            "owner_id", "last_evaluated_at", "last_error", "etag", "created_at", "updated_at")
    out = {k: row.get(k) for k in keys}
    out["builtin"] = bool(row.get("builtin_key"))
    return out


@router.get("/rule-types")
def rule_types(principal: Authed) -> dict[str, Any]:
    principal.require("events.read")
    return {"items": [{"type": t, "title": s["title"], "description": s["description"], "workspace_scoped": s["workspace"],
                       "params": {k: {kk: vv for kk, vv in v.items()} for k, v in s["params"].items()}} for t, s in RULE_TYPES.items()], "severities": list(SEVERITIES)}


# ---------------------------------------------------------------------------------------------------------------------------------------- rules
@router.get("/rules")
def list_rules(principal: Authed, ctx: Ctx, workspace_id: str | None = None) -> dict[str, Any]:
    svc = _svc(ctx)
    return {"items": [_rule_out(svc, r) for r in svc.list_rules(principal, workspace_id=workspace_id)]}


@router.post("/rules", status_code=201)
def create_rule(principal: Authed, ctx: Ctx, response: Response, body: RuleBody) -> dict[str, Any]:
    """Body: ``{name, type, severity?, params?, for_seconds?, repeat_seconds?, channels?, labels?, enabled?, workspace_id?, description?}``."""
    svc = _svc(ctx)
    row = svc.create_rule(principal, dump(body))
    response.headers["ETag"] = etag_header(row["etag"])
    return _rule_out(svc, row)


@router.get("/rules/{rule_id}")
def get_rule(rule_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    svc = _svc(ctx)
    row = svc.get_rule(rule_id)
    if not svc.can_read(principal, row["workspace_id"]):
        from olcf.core.errors import NotFound

        raise NotFound(f"alert rule {rule_id} not found")
    response.headers["ETag"] = etag_header(row["etag"])
    return _rule_out(svc, row)


@router.patch("/rules/{rule_id}")
def update_rule(rule_id: str, principal: Authed, ctx: Ctx, response: Response, body: RuleBody, if_match: IfMatch = None) -> dict[str, Any]:
    svc = _svc(ctx)
    check_if_match(if_match, svc.get_rule(rule_id)["etag"])
    row = svc.update_rule(principal, rule_id, dump(body))
    response.headers["ETag"] = etag_header(row["etag"])
    return _rule_out(svc, row)


@router.delete("/rules/{rule_id}", status_code=204)
def delete_rule(rule_id: str, principal: Authed, ctx: Ctx) -> Response:
    _svc(ctx).delete_rule(principal, rule_id)
    return Response(status_code=204)


@router.post("/rules/{rule_id}/test")
def test_rule(rule_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Evaluate the rule now without changing any alert; shows which instances would fire."""
    return _svc(ctx).test_rule(principal, rule_id)


@router.post("/rules/{rule_id}/silence")
def silence_rule(rule_id: str, principal: Authed, ctx: Ctx, body: SilenceBody | None = None) -> dict[str, Any]:
    """Body ``{"minutes": 60}`` silences notifications for that long; ``{"minutes": 0}`` lifts the silence."""
    svc = _svc(ctx)
    return _rule_out(svc, svc.silence_rule(principal, rule_id, _seconds(dump(body))))


# ---------------------------------------------------------------------------------------------------------------------------------------- channels
@router.get("/channels")
def list_channels(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    svc = _svc(ctx)
    return {"items": [svc.public_channel(c) for c in svc.list_channels(principal)]}


@router.post("/channels", status_code=201)
def create_channel(principal: Authed, ctx: Ctx, body: ChannelBody) -> dict[str, Any]:
    """Body: ``{name, type: log|webhook|email, config, workspace_id?, secret?, enabled?}`` (webhook ``config``: ``{url, headers?}``; email: ``{to: [..], subject_prefix?}``)."""
    svc = _svc(ctx)
    return svc.public_channel(svc.create_channel(principal, dump(body)))


@router.patch("/channels/{channel_id}")
def update_channel(channel_id: str, principal: Authed, ctx: Ctx, body: ChannelBody) -> dict[str, Any]:
    svc = _svc(ctx)
    return svc.public_channel(svc.update_channel(principal, channel_id, dump(body)))


@router.delete("/channels/{channel_id}", status_code=204)
def delete_channel(channel_id: str, principal: Authed, ctx: Ctx) -> Response:
    _svc(ctx).delete_channel(principal, channel_id)
    return Response(status_code=204)


@router.post("/channels/{channel_id}/test")
def test_channel(channel_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Send one test notification through the channel and report whether it was accepted."""
    return _svc(ctx).test_channel(principal, channel_id)


# ---------------------------------------------------------------------------------------------------------------------------------------- alerts
@router.post("/evaluate")
def evaluate(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("alert.manage")
    return _svc(ctx).evaluate_all(wait=True, force=True)


@router.get("")
def list_alerts(principal: Authed, ctx: Ctx, state: str | None = Query(None, description="comma separated: pending,firing,resolved"), severity: str | None = None,
                rule_id: str | None = None, since: str | None = None, limit: int = Query(100, ge=1, le=500), cursor: str | None = None) -> dict[str, Any]:
    return _svc(ctx).list_alerts(principal, state=state, severity=severity, rule_id=rule_id, since=since, limit=limit, cursor=cursor)


@router.get("/{alert_id}")
def get_alert(alert_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).get_alert(principal, alert_id)


@router.post("/{alert_id}/ack")
def ack_alert(alert_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).acknowledge(principal, alert_id)


@router.post("/{alert_id}/silence")
def silence_alert(alert_id: str, principal: Authed, ctx: Ctx, body: SilenceBody | None = None) -> dict[str, Any]:
    return _svc(ctx).silence(principal, alert_id, _seconds(dump(body)))


def _seconds(body: dict[str, Any]) -> float | None:
    from olcf.core.errors import ValidationFailed

    m = body.get("minutes")
    if m is None:
        m = 60
    if isinstance(m, bool) or not isinstance(m, (int, float)) or m < 0 or m > 90 * 1440:
        raise ValidationFailed("minutes must be a number between 0 and 129600")
    return float(m) * 60 or None
