# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST endpoints of the Fabric adapter, mounted at ``/v1/integrations/fabric`` (CONTRACTS section 5).

All endpoints authenticate (platform token or Entra token, see :mod:`olcf.fabric.auth`) and authorise server-side:

========================  ==========================================  ==========================================================
Endpoint                  Permission                                  Who typically calls it
========================  ==========================================  ==========================================================
GET /                     ``integration.fabric.read``                 administrators, the UI
PUT /                     ``integration.fabric.manage``               administrators
POST /test                ``integration.fabric.manage``               administrators (stepwise diagnostics)
POST /signing/rotate      ``integration.fabric.manage``               administrators
GET /metrics              ``integration.fabric.read``                 administrators, dashboards
POST /challenge           ``integration.fabric.submit`` (anywhere)    the SDK
POST /handshake           ``integration.fabric.submit`` (anywhere)    the SDK (``test_connection``)
POST /validate, /submit   ``integration.fabric.submit`` (workspace)   the SDK
GET /submissions[/id...]  ``job.read`` in the workspace (or fabric read)   the SDK, administrators
POST /submissions/id/cancel  ``job.cancel`` in the workspace          the SDK, administrators
POST /analyze, GET /compat/rules, GET /snippet                        any caller allowed to submit or read
========================  ==========================================  ==========================================================
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Ctx, client_ip
from olcf.core.errors import BadRequest, Forbidden, ValidationFailed
from olcf.fabric.analyzer import load_ruleset
from olcf.fabric.auth import FabricCaller
from olcf.fabric.errors import CapsuleTooLarge
from olcf.fabric.metrics import compute_metrics
from olcf.fabric.service import FabricService
from olcf.fabric.snippet import render as render_snippet

router = APIRouter(prefix="/integrations/fabric", tags=["fabric"])
MAX_ANALYZE_BYTES = 8 * 1024 * 1024
ENVELOPE_SLACK = 256 * 1024  # signature + JSON structure on top of the capsule limit


def svc(ctx: Ctx) -> FabricService:
    return ctx.service("fabric")


Svc = Annotated[FabricService, Depends(svc)]


async def _limited_body(request: Request, ctx: Ctx) -> bytes:
    """Read the request body, refusing early when it exceeds the capsule size limit (before JSON parsing and before any signature work)."""
    limit = int(ctx.settings.fabric.capsule_max_bytes) + ENVELOPE_SLACK
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise CapsuleTooLarge(f"the request is {declared} bytes; the limit is {limit}", remediation="submit by reference (pull mode) or shrink the notebook")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise CapsuleTooLarge(f"the request exceeds the limit of {limit} bytes", remediation="submit by reference (pull mode) or shrink the notebook")
        chunks.append(chunk)
    return b"".join(chunks)


Body = Annotated[bytes, Depends(_limited_body)]


def _json(raw: bytes) -> dict[str, Any]:
    try:
        doc = json.loads(raw or b"{}")
    except (ValueError, UnicodeDecodeError) as exc:
        raise BadRequest(f"the body is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise BadRequest("the body must be a JSON object")
    return doc


# ------------------------------------------------------------------------------------------------- configuration
@router.get("")
@router.get("/")
def get_integration(principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Configuration (no secrets), the readiness checklist and counters."""
    principal.require("integration.fabric.read")
    return service.config_public()


class ConfigBody(BaseModel):
    """The integration configuration (a ``GET`` result's ``config`` can be sent back unchanged). The fields mirror :class:`olcf.fabric.config.FabricConfig`; the strict validation
    (GUIDs, ranges, references) is that model's, applied by the service, which answers with field-path errors. Omitted fields take their closed/strict defaults."""

    model_config = ConfigDict(extra="allow")

    enabled: bool | None = None
    allowed_tenants: list[str] | None = Field(None, description="Entra tenant GUIDs that may submit")
    audiences: list[str] | None = None
    workspace_map: list[dict[str, Any]] | None = Field(None, description="[{fabric_workspace_id, olcf_workspace, ...}]")
    lakehouse_map: list[dict[str, Any]] | None = None
    broker_connection: str | None = Field(None, description="id of the OLCF onelake connection holding the broker service principal")
    default_runtime: dict[str, Any] | None = None
    default_policy: dict[str, Any] | None = None
    network: dict[str, Any] | None = None
    secret_grants: list[dict[str, Any]] | None = None
    auto_create_lakehouses: bool | None = None
    enforce_data_scope: bool | None = None
    pull_timeout_seconds: int | None = None
    sdk_hints: dict[str, Any] | None = None
    network_effective: dict[str, Any] | None = Field(None, description="computed, read-only; ignored on PUT")


@router.put("")
@router.put("/")
def put_integration(body: ConfigBody, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Replace the integration configuration (validated; references must exist)."""
    principal.require("integration.fabric.manage")
    return service.put_config(principal, body.model_dump(exclude_unset=True))


@router.post("/test")
def test_integration(principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Stepwise diagnostics: DNS -> TLS -> proxy -> Entra token -> Fabric API -> OneLake -> inbound path."""
    return service.diagnose(principal)


@router.post("/signing/rotate")
def rotate_signing(principal: FabricCaller, service: Svc) -> dict[str, Any]:
    return service.rotate_signing(principal)


@router.get("/metrics")
def metrics(principal: FabricCaller, ctx: Ctx, window: Annotated[str, Query(pattern="^(1h|24h|7d)$")] = "24h") -> dict[str, Any]:
    principal.require("integration.fabric.read")
    return compute_metrics(ctx, window)


# ------------------------------------------------------------------------------------------------- the SDK flow
@router.post("/challenge")
def challenge(principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Single-use nonce, per-submission HMAC key and server time. The key is returned once and never stored."""
    return service.challenge(principal)


@router.post("/handshake")
def handshake(raw: Body, request: Request, principal: FabricCaller, ctx: Ctx, service: Svc) -> dict[str, Any]:
    return service.handshake(principal, _json(raw), ip=client_ip(request, ctx))


@router.post("/validate")
def validate(raw: Body, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Pre-flight: everything ``/submit`` checks except the signature; no job is created. Blockers are reported in the body (``ok: false``)."""
    return service.validate(principal, _json(raw))


@router.post("/submit", status_code=202)
def submit(raw: Body, request: Request, principal: FabricCaller, ctx: Ctx, service: Svc) -> dict[str, Any]:
    """Accept a signed capsule. ``202`` with the submission and job ids; poll ``GET /submissions/{id}`` (or stream logs) for the result."""
    return service.submit(principal, _json(raw), ip=client_ip(request, ctx), raw_bytes=len(raw))


@router.get("/submissions")
def list_submissions(principal: FabricCaller, service: Svc, limit: Annotated[int, Query(ge=1, le=500)] = 50, state: str | None = None, notebook_id: str | None = None,
                     fabric_workspace_id: str | None = None, before: str | None = None) -> dict[str, Any]:
    rows = service.list_submissions(principal, limit=limit, state=state, notebook_id=notebook_id, fabric_workspace_id=fabric_workspace_id, before=before)
    return {"items": rows, "next_before": rows[-1]["received_at"] if len(rows) == limit else None}


@router.get("/submissions/{submission_id}")
def get_submission(submission_id: str, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    return service.get_submission(principal, submission_id)


@router.get("/submissions/{submission_id}/logs")
def submission_logs(submission_id: str, principal: FabricCaller, service: Svc, offset: Annotated[int, Query(ge=0)] = 0, limit: Annotated[int, Query(ge=1, le=1048576)] = 65536) -> dict[str, Any]:
    """Byte-offset paged job logs (the SDK streams them into the Fabric cell output)."""
    return service.submission_logs(principal, submission_id, offset=offset, limit=limit)


@router.get("/submissions/{submission_id}/output")
def submission_output(submission_id: str, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """What the notebook printed and returned per cell (from the executed output notebook), redacted and size-limited."""
    return service.submission_output(principal, submission_id)


@router.post("/submissions/{submission_id}/cancel")
def cancel_submission(submission_id: str, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    return service.cancel_submission(principal, submission_id)


# ------------------------------------------------------------------------------------------------- analyzer
class AnalyzeBody(BaseModel):
    """Request body for the analyze endpoint: notebook content, optional format, a what-if lakehouse mode and an optional runtime family. Unknown fields are rejected.
    """
    model_config = ConfigDict(extra="forbid")
    content: str
    format: str | None = None
    lakehouse: str | None = None  # what-if: "local" | "remote" | None
    runtime_family: str | None = None


@router.post("/analyze")
async def analyze(request: Request, principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """Analyse pasted (JSON ``{content, format}``) or uploaded (``multipart/form-data`` ``file``) notebook content. No job is created."""
    if not (principal.can("integration.fabric.read") or _anywhere(principal)):
        raise Forbidden("permission 'integration.fabric.submit' is required", details={"permission": "integration.fabric.submit"})
    ctype = request.headers.get("content-type", "")
    if ctype.startswith("multipart/form-data"):
        form = await request.form()
        up = form.get("file")
        if up is None or not hasattr(up, "read"):
            raise BadRequest("upload the notebook in the 'file' field")
        data = await up.read(MAX_ANALYZE_BYTES + 1)
        if len(data) > MAX_ANALYZE_BYTES:
            raise CapsuleTooLarge(f"the upload exceeds {MAX_ANALYZE_BYTES} bytes")
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BadRequest("the upload is not UTF-8 text") from exc
        fmt = (form.get("format") or None) if isinstance(form.get("format"), str) or form.get("format") is None else None
        kind = form.get("lakehouse") if isinstance(form.get("lakehouse"), str) else None
        family = None
    else:
        raw = await request.body()
        if len(raw) > MAX_ANALYZE_BYTES * 2:
            raise CapsuleTooLarge("the request body is too large")
        body = AnalyzeBody.model_validate(_json(raw)) if raw else None
        if body is None:
            raise ValidationFailed("send {\"content\": \"...\", \"format\": \"ipynb|fabric_git_source|py\"}")
        text, fmt, kind, family = body.content, body.format, body.lakehouse, body.runtime_family
    return service.analyze_document(text, format=fmt, lakehouse_kind=kind, runtime_family=family)


def _anywhere(principal: Any) -> bool:
    vis = principal.visible_workspaces("integration.fabric.submit")
    return vis is None or bool(vis)


@router.get("/compat/rules")
def compat_rules(principal: FabricCaller, service: Svc) -> dict[str, Any]:
    """The versioned rule set (class, message, remediation, matchers) the analyzer applies."""
    if not (principal.can("integration.fabric.read") or _anywhere(principal)):
        raise Forbidden("permission 'integration.fabric.read' is required", details={"permission": "integration.fabric.read"})
    return load_ruleset().describe()


@router.get("/snippet")
def snippet(principal: FabricCaller, service: Svc, workspace: str | None = None, auth: Annotated[str, Query(pattern="^(keyvault|token|client_credentials|notebookutils)$")] = "keyvault") -> dict[str, Any]:
    """A cell to paste into a Fabric notebook. ``workspace``: a Fabric workspace id (shows how it is mapped) or an OLCF workspace name."""
    if not (principal.can("integration.fabric.read") or _anywhere(principal)):
        raise Forbidden("permission 'integration.fabric.submit' is required", details={"permission": "integration.fabric.submit"})
    cfg = service.config()
    olcf_ws = None
    fabric_ws = None
    if workspace:
        m = cfg.workspace_for(workspace)
        if m is not None:
            fabric_ws, olcf_ws = m.fabric_workspace_id, m.olcf_workspace
        else:
            fabric_ws = workspace
    aud = cfg.audiences[0] if cfg.audiences else None
    return render_snippet(endpoint=service.public_url(), fabric_workspace=fabric_ws, olcf_workspace=olcf_ws, auth=auth, audience=aud)
