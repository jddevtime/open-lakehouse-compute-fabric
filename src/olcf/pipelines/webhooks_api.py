# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of outbound webhooks (spec 24), mounted at ``/v1/webhooks``.

``GET /event-types``                          the event catalogue (``job.* pipeline.* item.* worker.* security.* fabric.* ...``) and the delivery contract
``GET|POST /``, ``GET|PATCH|DELETE /{id}``    subscriptions; the signing secret is returned **once** (on creation when generated, and on rotation)
``POST /{id}/test``                           synchronous test delivery (``webhook.test`` event); the recorded delivery is returned
``POST /{id}/rotate-secret``
``GET /{id}/deliveries``                      delivery log (filter ``state=dead`` for the dead-letter queue)
``GET /deliveries/{delivery_id}``             one delivery with payload and per-attempt log; ``POST /deliveries/{delivery_id}/redeliver``

Authorisation: ``webhook.manage`` (in the subscription's workspace, or organisation-wide for subscriptions without a workspace). Target URLs pass the SSRF guard.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response
from pydantic import Field

from olcf.api.deps import Authed, Ctx, IfMatch, check_if_match, etag_header
from olcf.core.errors import Unavailable
from olcf.core.models import OpenBody, dump
from olcf.pipelines.webhooks import EVENT_CATALOG

router = APIRouter(tags=["webhooks"])


class WebhookBody(OpenBody):
    """Create / update an outbound webhook subscription (``name``, ``url`` and ``event_types`` are required on create; the service validates them, including the SSRF guard)."""

    name: str | None = None
    url: str | None = Field(None, description="target URL (http/https; must pass the SSRF guard)")
    event_types: list[str] | None = Field(None, description="fnmatch patterns against the event type, e.g. job.*, pipeline.run.failed, *")
    workspace_id: str | None = Field(None, description="omit for an organisation-wide subscription")
    secret: str | None = Field(None, description="signing secret; generated (and shown once) when omitted")
    headers: dict[str, Any] | None = Field(None, description="extra request headers (values are never returned)")
    enabled: bool | None = None
    max_attempts: int | None = Field(None, description="delivery attempts before a delivery is dead (default 8)")
    timeout_seconds: float | None = Field(None, description="per-attempt timeout (default 10)")
    description: str | None = None


class RotateSecretBody(OpenBody):
    """Request body for rotating a webhook secret; a new secret is generated when none is given."""
    secret: str | None = Field(None, description="the new secret; generated when omitted")


def _svc(ctx: Any) -> Any:
    svc = ctx.maybe_service("webhooks")
    if svc is None:
        raise Unavailable("the webhook service is not running")
    return svc


@router.get("/event-types")
def event_types(principal: Authed) -> dict[str, Any]:
    return {"items": EVENT_CATALOG, "patterns": "fnmatch against the event type, e.g. job.*, pipeline.run.failed, *",
            "delivery": {"method": "POST", "content_type": "application/json", "signature": "X-OLCF-Signature: sha256=HMAC_SHA256(secret, '<X-OLCF-Timestamp>.<body>')",
                         "headers": ["X-OLCF-Event", "X-OLCF-Event-Id", "X-OLCF-Delivery", "X-OLCF-Attempt", "X-OLCF-Timestamp", "X-OLCF-Signature"],
                         "success": "any 2xx", "retry": "408, 425, 429, 5xx and network errors with exponential backoff; other 4xx are dead; 410 disables the subscription"}}


@router.get("")
def list_webhooks(principal: Authed, ctx: Ctx, workspace_id: str | None = None) -> dict[str, Any]:
    svc = _svc(ctx)
    return {"items": [svc.public(r) for r in svc.list(principal, workspace_id)]}


@router.post("", status_code=201)
def create_webhook(principal: Authed, ctx: Ctx, response: Response, body: WebhookBody) -> dict[str, Any]:
    """Body: ``{name, url, event_types: ["job.*", ...], workspace_id?, secret?, headers?, enabled?, max_attempts?: 8, timeout_seconds?: 10, description?}``."""
    svc = _svc(ctx)
    row, shown = svc.create(principal, dump(body))
    response.headers["ETag"] = etag_header(row["etag"])
    out = svc.public(row)
    if shown:
        out.update(shown)  # {"secret": ...} - only when the server generated it
    return out


@router.get("/deliveries/{delivery_id}")
def get_delivery(delivery_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    svc = _svc(ctx)
    return svc.public_delivery(svc.get_delivery(principal, delivery_id), include_payload=True)


@router.post("/deliveries/{delivery_id}/redeliver", status_code=202)
def redeliver(delivery_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    svc = _svc(ctx)
    return svc.public_delivery(svc.redeliver(principal, delivery_id))


@router.get("/{webhook_id}")
def get_webhook(webhook_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    svc = _svc(ctx)
    row = svc.get_authorized(principal, webhook_id)
    response.headers["ETag"] = etag_header(row["etag"])
    return svc.public(row)


@router.patch("/{webhook_id}")
def update_webhook(webhook_id: str, principal: Authed, ctx: Ctx, response: Response, body: WebhookBody, if_match: IfMatch = None) -> dict[str, Any]:
    svc = _svc(ctx)
    check_if_match(if_match, svc.get_authorized(principal, webhook_id)["etag"])
    row = svc.update(principal, webhook_id, dump(body))
    response.headers["ETag"] = etag_header(row["etag"])
    return svc.public(row)


@router.delete("/{webhook_id}", status_code=204)
def delete_webhook(webhook_id: str, principal: Authed, ctx: Ctx) -> Response:
    _svc(ctx).delete(principal, webhook_id)
    return Response(status_code=204)


@router.post("/{webhook_id}/test")
def test_webhook(webhook_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Send a ``webhook.test`` event now and report the outcome (state, status code, latency, error)."""
    svc = _svc(ctx)
    return svc.public_delivery(svc.test(principal, webhook_id), include_payload=True)


@router.post("/{webhook_id}/rotate-secret")
def rotate_secret(webhook_id: str, principal: Authed, ctx: Ctx, body: RotateSecretBody | None = None) -> dict[str, Any]:
    """Rotate the signing secret (optionally supply it with ``{"secret": "..."}``); the new value is shown once."""
    return _svc(ctx).rotate_secret(principal, webhook_id, dump(body).get("secret"))


@router.get("/{webhook_id}/deliveries")
def list_deliveries(webhook_id: str, principal: Authed, ctx: Ctx, state: str | None = Query(None, description="comma separated: pending,retrying,delivering,succeeded,dead"),
                    event_type: str | None = None, limit: int = Query(50, ge=1, le=500), cursor: str | None = None) -> dict[str, Any]:
    return _svc(ctx).deliveries(principal, webhook_id, state=state, event_type=event_type, limit=limit, cursor=cursor)
