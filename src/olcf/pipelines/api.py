# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of pipelines, runs, triggers and schedules (CONTRACTS section 4.1, orchestration addenda).

``/v1/pipelines``      ``GET /activity-types``, ``GET /schema``, ``POST /validate``, ``POST /import-fabric``, ``GET /{id}/definition``, ``POST /{id}/runs``, ``GET /{id}/runs``,
                       ``GET /{id}/triggers``. Pipelines themselves are *items* (create/update/delete through ``/v1/workspaces/{ws}/items``).
``/v1/pipeline-runs``  ``GET`` (list), ``GET /{id}``, ``GET /{id}/graph`` (run graph with timings), ``GET /{id}/activities[/{aid}]``, ``GET /{id}/children``,
                       ``POST /{id}/cancel``, ``POST /{id}/resume`` (resume from failure / re-run selected activities), ``POST /{id}/rerun`` (full re-run).
``/v1/triggers``       webhook / storage-event / dependency triggers (CRUD, enable, disable, pause, run-now, rotate-secret, runs) and read access to schedule triggers.
``/v1/schedules``      schedule items with their next fire times: ``POST /preview``, ``GET``, ``GET /{item}``, ``POST /{item}/enable|disable|pause|resume|run-now``, ``GET /{item}/next|runs``.
``/v1/hooks/pipelines/{trigger_id}`` (public, signed) and ``/v1/hooks/pipelines/callbacks/{activity_run_id}/{token}`` (public, token is the credential).

Authorisation: ``pipeline.run`` to start / cancel / resume / run-now; ``item.read`` to look at pipelines, runs and triggers; ``item.create|update|delete`` to manage
triggers of a workspace. Responses never contain secret values (webhook secrets and bearer tokens are shown once, on creation / rotation).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import Field, StrictFloat, StrictInt

from olcf.api.deps import Authed, Ctx, IdempotencyKey
from olcf.core.errors import BadRequest, NotFound, OlcfError, Unavailable, ValidationFailed
from olcf.core.models import OpenBody, dump
from olcf.core.timeutil import from_iso, to_iso
from olcf.pipelines import model as M
from olcf.pipelines.fabric_import import FabricImportError, import_fabric_pipeline
from olcf.pipelines.triggers import MAX_BODY_BYTES, parse_tz, validate_cron

pipelines_router = APIRouter(tags=["pipelines"])
workspace_pipelines_router = APIRouter(tags=["pipelines"])
runs_router = APIRouter(tags=["pipeline-runs"])
triggers_router = APIRouter(tags=["triggers"])
schedules_router = APIRouter(tags=["schedules"])
hooks_router = APIRouter(tags=["hooks"])


# ---------------------------------------------------------------------------------------------------------------------------------------- request bodies
# Known fields are typed for OpenAPI / generated clients; the services still validate the semantics and unknown keys still reach them (see ``olcf.core.models.OpenBody``).
class ValidateBody(OpenBody):
    """Request body for validating a pipeline definition, given as an object or as JSON or YAML text."""
    definition: dict[str, Any] | None = Field(None, description="a pipeline definition object")
    text: str | None = Field(None, description="a definition as JSON or YAML text (alternative to ``definition``)")
    format: str | None = Field(None, description="json | yaml, the format of ``text``")


class ImportFabricBody(OpenBody):
    """Request body for importing a Fabric or Data Factory pipeline document, with an optional name and a map from Fabric items to OLCF items.
    """
    document: dict[str, Any] | None = Field(None, description="the Fabric / Data Factory pipeline document")
    text: str | None = Field(None, description="the document as JSON text (alternative to ``document``)")
    name: str | None = None
    item_map: dict[str, str] | None = Field(None, description="Fabric item id or name -> OLCF item name or id")
    on_unsupported: str | None = Field(None, description="placeholder (default) | skip | error")
    workspace_id: str | None = None
    create: bool | None = Field(None, description="create a pipeline item from a valid result (needs workspace_id)")


class StartRunBody(OpenBody):
    """Request body for starting a pipeline run: parameters and what to do if a run is already active (queue, skip or cancel the previous one).
    """
    parameters: dict[str, Any] | None = None
    queue_policy: str | None = Field(None, description="queue | skip | cancel_previous")


class CancelRunBody(OpenBody):
    """Request body for cancelling a pipeline run, with an optional reason."""
    reason: str | None = None


class ResumeRunBody(OpenBody):
    """Request body for resuming a failed run: activities to execute again, new parameters, whether to use the latest definition and whether to reuse successful results.
    """
    rerun: list[str] | None = Field(None, description="activities to execute again (and everything that depends on them)")
    parameters: dict[str, Any] | None = None
    latest_definition: bool | None = Field(None, description="use the current pipeline definition instead of the one of the run (default false)")
    reuse: bool | None = Field(None, description="re-use the successful activity results (default true)")


class RerunBody(OpenBody):
    """Request body for running a pipeline again from an earlier run, with optional parameters and the latest definition."""
    parameters: dict[str, Any] | None = None
    latest_definition: bool | None = None


class TriggerTarget(OpenBody):
    """What a trigger starts: a pipeline, notebook or Spark job definition, given by item id or name."""
    type: str | None = Field(None, description="pipeline | notebook | spark_job_definition")
    ref: str | None = Field(None, description="item id or name")


class TriggerBody(OpenBody):
    """Create / update a webhook, storage_event or dependency trigger (``workspace_id``, ``name``, ``type`` and ``target`` are required on create; ``type`` and ``target`` cannot be changed)."""

    workspace_id: str | None = None
    name: str | None = None
    type: str | None = Field(None, description="webhook | storage_event | dependency")
    target: TriggerTarget | None = None
    parameters: dict[str, Any] | None = None
    config: dict[str, Any] | None = Field(None, description="type specific settings (webhook: auth hmac|bearer, ...)")
    enabled: bool | None = None


class PauseBody(OpenBody):
    """Request body for pausing a schedule or trigger for a number of minutes or until a time."""
    minutes: StrictInt | StrictFloat | None = Field(None, description="pause for this many minutes (up to 525600)")
    until: str | None = Field(None, description="pause until this ISO-8601 time (alternative to minutes)")


class PreviewBody(OpenBody):
    """Request body for previewing the next fire times of a cron expression in a time zone."""
    cron: str | None = Field(None, description="cron expression")
    timezone: str | None = Field(None, description="IANA time zone (default UTC)")
    count: int | None = Field(None, description="1-100 fire times (default 5)")
    after: str | None = Field(None, description="ISO-8601 start (default now)")


class ScheduleCreateBody(OpenBody):
    """Request body for creating a schedule: workspace, name, description and a definition with the cron expression, time zone and target.
    """
    workspace_id: str | None = None
    name: str | None = None
    description: str | None = None
    definition: dict[str, Any] | None = Field(None, description="{cron, timezone, target, catchup, ...}")


class PayloadTooLarge(OlcfError):
    """The request body exceeds the allowed size (HTTP 413, code ``payload_too_large``)."""
    status = 413
    code = "payload_too_large"


def _svc(ctx: Any) -> Any:
    svc = ctx.maybe_service("pipelines")
    if svc is None:
        raise Unavailable("the pipeline service is not running")
    return svc


def _trig(ctx: Any) -> Any:
    return _svc(ctx).triggers


def _item(ctx: Any, principal: Any, item_id: str, perm: str = "item.read", type_: str | None = None) -> dict[str, Any]:
    item = ctx.service("items").get(item_id)
    if type_ and item["type"] != type_:
        raise NotFound(f"{type_} {item_id} not found")
    principal.require(perm, workspace_id=item["workspace_id"], item_id=item["id"])
    return item


# ---------------------------------------------------------------------------------------------------------------------------------------- pipelines
@pipelines_router.get("/activity-types")
def activity_types(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The installed activity types (built-ins and plugins) with their property schemas, plus the expression function catalogue."""
    from olcf.pipelines.expressions import FUNCTIONS

    svc = _svc(ctx)
    return {"items": svc.activity_types(), "plugins": svc.registry.plugins, "rejected_plugins": svc.registry.rejected,
            "functions": [{"name": n, "doc": f.doc, "min_args": f.min_args, "max_args": f.max_args} for n, f in sorted(FUNCTIONS.items())]}


@pipelines_router.get("/schema")
def pipeline_schema(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """JSON Schema (draft 2020-12) of a pipeline definition, generated from the installed activity types."""
    return _svc(ctx).schema()


@pipelines_router.post("/validate")
def validate(principal: Authed, ctx: Ctx, body: ValidateBody) -> dict[str, Any]:
    """Validate a definition (``{"definition": {...}}`` or ``{"text": "<json|yaml>", "format": "json|yaml"}``). Always 200; ``valid`` and ``errors`` tell the result."""
    return _svc(ctx).validate(dump(body))


@pipelines_router.post("/import-fabric")
def import_fabric(principal: Authed, ctx: Ctx, body_in: ImportFabricBody) -> dict[str, Any]:
    """Translate a Microsoft Fabric / Data Factory pipeline document.

    Body: ``{"document": {...} | "text": "<json>", "name"?, "item_map"?: {"<fabric id or name>": "<OLCF item name or id>"}, "on_unsupported"?: "placeholder|skip|error",
    "workspace_id"?, "create"?: false}``. The response carries ``definition``, ``validation`` and an explicit ``report`` (unsupported activities with reasons and remediation, warnings,
    activity mapping, unresolved references). With ``create=true`` (and a ``workspace_id``) and a valid result, a pipeline item is created.
    """
    body = dump(body_in)
    svc = _svc(ctx)
    doc = body.get("document")
    if doc is None and body.get("text"):
        doc = M.parse_definition_text(body["text"], "json")
    if not isinstance(doc, dict):
        raise BadRequest("provide 'document' (object) or 'text' (JSON)")
    item_map = body.get("item_map") or {}
    if not isinstance(item_map, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in item_map.items()):
        raise ValidationFailed("item_map must map strings to strings")
    ws = body.get("workspace_id")
    if ws:
        principal.require("item.read", workspace_id=ws)
    try:
        res = import_fabric_pipeline(doc, svc.registry, item_map=item_map, on_unsupported=str(body.get("on_unsupported") or "placeholder"), name=body.get("name"))
    except FabricImportError as exc:
        raise ValidationFailed(f"the pipeline cannot be imported: {exc}", remediation="use on_unsupported=placeholder or skip to import the supported part") from None
    except ValueError as exc:
        raise BadRequest(str(exc)) from None
    out = {**res, "created": None}
    if body.get("create"):
        if not ws:
            raise ValidationFailed("workspace_id is required to create the pipeline item")
        principal.require("item.create", workspace_id=ws)
        if not res["validation"]["valid"]:
            raise ValidationFailed("the imported definition is not valid, so no item was created", details={"errors": res["validation"]["errors"]})
        from olcf.core.models import ItemCreate

        name = str(body.get("name") or res.get("name") or "").strip()
        if not name:
            raise ValidationFailed("name is required to create the pipeline item")
        item = ctx.service("items").create(principal, ws, ItemCreate(type="pipeline", name=name, definition=res["definition"], description=f"Imported from Microsoft Fabric ({res['report']['imported']} activities)"))
        out["created"] = {"id": item["id"], "name": item["name"], "workspace_id": ws}
    return out


@pipelines_router.get("/{pipeline_id}/definition")
def get_definition(pipeline_id: str, principal: Authed, ctx: Ctx, format: str = Query("json", pattern="^(json|yaml)$")) -> Any:  # noqa: A002
    item = _item(ctx, principal, pipeline_id, type_="pipeline")
    res = M.validate_definition(item.get("definition") or {}, _svc(ctx).registry)
    if format == "yaml":
        return Response(content=M.dump_definition(res.definition if res.ok else item["definition"], "yaml"), media_type="application/yaml")
    return {"id": item["id"], "name": item["name"], "version": item["version"], "etag": item["etag"], "definition": res.definition if res.ok else item["definition"],
            "validation": {"valid": res.ok, "errors": [i.to_dict() for i in res.errors], "warnings": [i.to_dict() for i in res.warnings]}, "graph": res.graph}


@pipelines_router.post("/{pipeline_id}/runs", status_code=202)
def start_run(pipeline_id: str, principal: Authed, ctx: Ctx, response: Response, idem: IdempotencyKey = None, body_in: StartRunBody | None = None) -> dict[str, Any]:
    """Start a run. Body: ``{"parameters": {...}, "queue_policy"?: "queue|skip|cancel_previous"}``. Replays of the same ``Idempotency-Key`` return the original run."""
    body = dump(body_in)
    item = _item(ctx, principal, pipeline_id, "pipeline.run", "pipeline")
    run = _svc(ctx).run(principal, item["workspace_id"], item["id"], body.get("parameters") or {}, idempotency_key=idem, queue_policy=body.get("queue_policy"))
    response.headers["Location"] = f"/v1/pipeline-runs/{run['id']}"
    return run


@workspace_pipelines_router.get("/{ws}/pipelines")
def workspace_pipelines(ws: str, principal: Authed, ctx: Ctx, q: str | None = Query(None, description="substring of name or description"),
                        include: str | None = Query(None, description="comma separated; 'definition' embeds each readable definition (size-capped, see definition_omitted)"),
                        limit: int = Query(50, ge=1, le=200), cursor: str | None = None) -> dict[str, Any]:
    """The pipelines of a workspace with their last run and trigger summary, in one call (a list page needs no per-pipeline requests).

    Each item: ``{id, name, folder, description, tags, version, etag, created_at, updated_at, last_run: {id, state, started_at, finished_at, duration_seconds, ...} | null,
    triggers: {total, enabled, by_type, next_fire}}``; with ``?include=definition`` also ``definition``.
    """
    wanted = {x.strip() for x in (include or "").split(",") if x.strip()}
    unknown = wanted - {"definition"}
    if unknown:
        raise BadRequest(f"unsupported include value(s) {sorted(unknown)}; supported: definition")
    row = ctx.service("workspaces").resolve(ws)
    return _svc(ctx).workspace_pipelines(principal, row["id"], q=q, include_definition="definition" in wanted, limit=limit, cursor=cursor)


@pipelines_router.get("/{pipeline_id}/runs")
def pipeline_runs(pipeline_id: str, principal: Authed, ctx: Ctx, state: str | None = None, limit: int = Query(50, ge=1, le=500), cursor: str | None = None) -> dict[str, Any]:
    item = _item(ctx, principal, pipeline_id, type_="pipeline")
    return _svc(ctx).list_runs(principal, workspace_id=item["workspace_id"], pipeline_id=item["id"], state=state, limit=limit, cursor=cursor)


@pipelines_router.get("/{pipeline_id}/triggers")
def pipeline_triggers(pipeline_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    item = _item(ctx, principal, pipeline_id, type_="pipeline")
    t = _trig(ctx)
    return {"items": [t.public(r) for r in t.list(item["workspace_id"], target_id=item["id"])]}


# ---------------------------------------------------------------------------------------------------------------------------------------- runs
@runs_router.get("")
def list_runs(principal: Authed, ctx: Ctx, workspace_id: str | None = None, pipeline_id: str | None = None, state: str | None = Query(None, description="comma separated"),
              trigger_type: str | None = None, parent_run_id: str | None = None, group_id: str | None = None, since: str | None = None, limit: int = Query(50, ge=1, le=500),
              cursor: str | None = None) -> dict[str, Any]:
    return _svc(ctx).list_runs(principal, workspace_id=workspace_id, pipeline_id=pipeline_id, state=state, trigger_type=trigger_type, parent_run_id=parent_run_id, group_id=group_id,
                               since=since, limit=limit, cursor=cursor)


@runs_router.get("/{run_id}")
def get_run(run_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).get_run(principal, run_id)


@runs_router.get("/{run_id}/graph")
def run_graph(run_id: str, principal: Authed, ctx: Ctx, include_io: bool = False) -> dict[str, Any]:
    """The run graph: nodes (state, attempts, timings, instances, job and log links), edges (with conditions), timeline and critical path."""
    return _svc(ctx).graph(principal, run_id, include_io=include_io)


@runs_router.get("/{run_id}/activities")
def run_activities(run_id: str, principal: Authed, ctx: Ctx, include_io: bool = False) -> dict[str, Any]:
    return {"items": _svc(ctx).activities(principal, run_id, include_io=include_io)}


@runs_router.get("/{run_id}/activities/{activity_id}")
def run_activity(run_id: str, activity_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """One activity run with its (redacted) input, output, error and attempts."""
    return _svc(ctx).activity(principal, run_id, activity_id)


@runs_router.get("/{run_id}/children")
def run_children(run_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return {"items": _svc(ctx).children(principal, run_id)}


@runs_router.post("/{run_id}/cancel", status_code=202)
def cancel_run(run_id: str, principal: Authed, ctx: Ctx, body_in: CancelRunBody | None = None) -> dict[str, Any]:
    """Request cancellation; running activities (jobs, queries, child runs) are cancelled and the run ends ``cancelled``."""
    body = dump(body_in)
    return _svc(ctx).cancel(principal, run_id, body.get("reason"))


@runs_router.post("/{run_id}/resume", status_code=202)
def resume_run(run_id: str, principal: Authed, ctx: Ctx, response: Response, idem: IdempotencyKey = None, body_in: ResumeRunBody | None = None) -> dict[str, Any]:
    """Resume from failure: a new run (same group) that re-uses the successful activity results of ``run_id``.

    Body: ``{"rerun": ["ActivityName", ...]?, "parameters"?: {...}, "latest_definition"?: false, "reuse"?: true}``. Activities named in ``rerun`` (and everything that depends
    on them) execute again; changed parameters disable reuse because the old results no longer apply.
    """
    body = dump(body_in)
    run = _svc(ctx).resume(principal, run_id, rerun=body.get("rerun"), parameters=body.get("parameters"), latest_definition=bool(body.get("latest_definition", False)),
                           reuse=bool(body.get("reuse", True)), idempotency_key=idem)
    response.headers["Location"] = f"/v1/pipeline-runs/{run['id']}"
    return run


@runs_router.post("/{run_id}/rerun", status_code=202)
def rerun(run_id: str, principal: Authed, ctx: Ctx, response: Response, idem: IdempotencyKey = None, body_in: RerunBody | None = None) -> dict[str, Any]:
    """Full re-run with the same (or overridden) parameters; nothing is reused."""
    body = dump(body_in)
    run = _svc(ctx).resume(principal, run_id, parameters=body.get("parameters"), latest_definition=bool(body.get("latest_definition", False)), reuse=False, idempotency_key=idem)
    response.headers["Location"] = f"/v1/pipeline-runs/{run['id']}"
    return run


# ---------------------------------------------------------------------------------------------------------------------------------------- triggers
def _trigger_for(ctx: Any, principal: Any, trigger_id: str, perm: str) -> dict[str, Any]:
    row = _trig(ctx).get(trigger_id)
    principal.require(perm, workspace_id=row["workspace_id"])
    return row


@triggers_router.get("")
def list_triggers(principal: Authed, ctx: Ctx, workspace_id: str = Query(...), type: str | None = None, target_id: str | None = None) -> dict[str, Any]:  # noqa: A002
    principal.require("item.read", workspace_id=workspace_id)
    t = _trig(ctx)
    return {"items": [t.public(r) for r in t.list(workspace_id, type_=type, target_id=target_id)]}


@triggers_router.post("", status_code=201)
def create_trigger(principal: Authed, ctx: Ctx, body_in: TriggerBody) -> dict[str, Any]:
    """Create a webhook / storage_event / dependency trigger.

    Body: ``{"workspace_id", "name", "type": "webhook|storage_event|dependency", "target": {"type": "pipeline|notebook|spark_job_definition", "ref": "<id or name>"},
    "parameters"?: {...}, "config"?: {...}, "enabled"?}``. Webhook triggers answer with ``secret`` (HMAC key) or ``token`` (bearer) **once**, plus the ``url`` to call.
    """
    body = dump(body_in)
    ws = body.get("workspace_id")
    if not ws:
        raise ValidationFailed("workspace_id is required")
    principal.require("item.create", workspace_id=ws)
    t = _trig(ctx)
    row, shown = t.create(principal, ws, body)
    return {**t.public(row), "credentials": shown}


@triggers_router.get("/{trigger_id}")
def get_trigger(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _trig(ctx).public(_trigger_for(ctx, principal, trigger_id, "item.read"))


@triggers_router.patch("/{trigger_id}")
def update_trigger(trigger_id: str, principal: Authed, ctx: Ctx, body_in: TriggerBody) -> dict[str, Any]:
    body = dump(body_in)
    _trigger_for(ctx, principal, trigger_id, "item.update")
    t = _trig(ctx)
    return t.public(t.update(trigger_id, body, principal))


@triggers_router.delete("/{trigger_id}", status_code=204)
def delete_trigger(trigger_id: str, principal: Authed, ctx: Ctx) -> Response:
    row = _trigger_for(ctx, principal, trigger_id, "item.delete")
    _trig(ctx).delete(trigger_id)
    ctx.audit.record(principal, "trigger.deleted", resource_type="trigger", resource_id=trigger_id, workspace_id=row["workspace_id"])
    return Response(status_code=204)


@triggers_router.post("/{trigger_id}/enable")
def enable_trigger(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _trigger_for(ctx, principal, trigger_id, "item.update")
    t = _trig(ctx)
    return t.public(t.set_enabled(principal, trigger_id, True))


@triggers_router.post("/{trigger_id}/disable")
def disable_trigger(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _trigger_for(ctx, principal, trigger_id, "item.update")
    t = _trig(ctx)
    return t.public(t.set_enabled(principal, trigger_id, False))


def _until(body: dict[str, Any]) -> datetime | None:
    now = datetime.now(UTC)
    if body.get("until"):
        try:
            u = from_iso(str(body["until"]))
        except ValueError:
            raise ValidationFailed("until must be an ISO-8601 timestamp") from None
        return u if u.tzinfo else u.replace(tzinfo=UTC)
    m = body.get("minutes")
    if m is None:
        raise ValidationFailed("provide minutes or until (use POST .../resume to lift a pause)")
    if isinstance(m, bool) or not isinstance(m, (int, float)) or m <= 0 or m > 525600:
        raise ValidationFailed("minutes must be a positive number up to 525600")
    return now + timedelta(minutes=float(m))


@triggers_router.post("/{trigger_id}/pause")
def pause_trigger(trigger_id: str, principal: Authed, ctx: Ctx, body_in: PauseBody) -> dict[str, Any]:
    """Pause until a time (``{"minutes": 60}`` or ``{"until": "<iso>"}``); the paused period is not replayed afterwards."""
    _trigger_for(ctx, principal, trigger_id, "item.update")
    t = _trig(ctx)
    return t.public(t.pause(trigger_id, _until(dump(body_in))))


@triggers_router.post("/{trigger_id}/resume")
def resume_trigger(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _trigger_for(ctx, principal, trigger_id, "item.update")
    t = _trig(ctx)
    return t.public(t.pause(trigger_id, None))


@triggers_router.post("/{trigger_id}/run-now", status_code=202)
def run_trigger_now(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Fire the trigger immediately as the caller (the cron / event condition is ignored)."""
    _trigger_for(ctx, principal, trigger_id, "pipeline.run")
    return _trig(ctx).run_now(principal, trigger_id)


@triggers_router.post("/{trigger_id}/rotate-secret")
def rotate_trigger_secret(trigger_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = _trigger_for(ctx, principal, trigger_id, "item.update")
    out = _trig(ctx).rotate_secret(trigger_id)
    ctx.audit.record(principal, "trigger.secret_rotated", resource_type="trigger", resource_id=trigger_id, workspace_id=row["workspace_id"])
    return out


@triggers_router.get("/{trigger_id}/runs")
def trigger_runs(trigger_id: str, principal: Authed, ctx: Ctx, limit: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
    """Firing history: one row per claimed fire time / delivery (status started, skipped, failed) with the run or job it started."""
    _trigger_for(ctx, principal, trigger_id, "item.read")
    return {"items": _trig(ctx).runs(trigger_id, limit)}


@triggers_router.get("/{trigger_id}/next")
def trigger_next(trigger_id: str, principal: Authed, ctx: Ctx, count: int = Query(5, ge=1, le=100)) -> dict[str, Any]:
    row = _trigger_for(ctx, principal, trigger_id, "item.read")
    return {"items": _trig(ctx).preview(row, count)}


# ---------------------------------------------------------------------------------------------------------------------------------------- schedules
def _schedule_trigger(ctx: Any, principal: Any, item_id: str, perm: str) -> tuple[dict[str, Any], dict[str, Any]]:
    item = _item(ctx, principal, item_id, perm, "schedule")
    t = _trig(ctx)
    rows = [r for r in t.list(item["workspace_id"], type_="schedule") if r["item_id"] == item["id"]]
    if not rows:
        t.reconcile_schedules()
        rows = [r for r in t.list(item["workspace_id"], type_="schedule") if r["item_id"] == item["id"]]
    if not rows:
        raise NotFound(f"schedule {item_id} has no active trigger (is its definition valid?)")
    return item, rows[0]


def _schedule_out(ctx: Any, item: dict[str, Any], trg: dict[str, Any] | None) -> dict[str, Any]:
    t = _trig(ctx)
    out: dict[str, Any] = {"id": item["id"], "workspace_id": item["workspace_id"], "name": item["name"], "definition": item["definition"], "etag": item["etag"], "updated_at": item["updated_at"]}
    if trg:
        cursor = (trg.get("state") or {}).get("cursor")
        out["trigger"] = t.public(trg)
        out["enabled"] = bool(trg["enabled"])
        out["paused_until"] = trg.get("paused_until")
        try:
            out["next_fire_times"] = t.preview(trg, 3, from_iso(cursor) if cursor else None)
        except Exception:  # noqa: BLE001
            out["next_fire_times"] = []
    return out


@schedules_router.post("/preview")
def preview(principal: Authed, ctx: Ctx, body_in: PreviewBody) -> dict[str, Any]:
    """Next fire times of a cron expression in a time zone: ``{"cron", "timezone"?, "count"?: 5, "after"?: "<iso>"}`` (stateless; nothing is stored)."""
    body = dump(body_in)
    from olcf.pipelines.triggers import next_fire_times

    t = _trig(ctx)
    validate_cron(body.get("cron"), float(t._setting("pipelines.min_schedule_interval_seconds", 60)))  # noqa: SLF001
    tz = parse_tz(body.get("timezone"))
    count = int(body.get("count") or 5)
    if not 1 <= count <= 100:
        raise ValidationFailed("count must be between 1 and 100")
    after = from_iso(body["after"]) if body.get("after") else t.now()
    return {"timezone": str(tz), "items": [{"utc": to_iso(x), "local": x.astimezone(tz).isoformat()} for x in next_fire_times(body["cron"], tz, after, count)]}


@schedules_router.get("")
def list_schedules(principal: Authed, ctx: Ctx, workspace_id: str = Query(...)) -> dict[str, Any]:
    principal.require("item.read", workspace_id=workspace_id)
    rows, _ = ctx.service("items").list(workspace_id, type_="schedule", limit=500, with_definition=True)
    t = _trig(ctx)
    by_item = {r["item_id"]: r for r in t.list(workspace_id, type_="schedule")}
    return {"items": [_schedule_out(ctx, i, by_item.get(i["id"])) for i in rows]}


@schedules_router.post("", status_code=201)
def create_schedule(principal: Authed, ctx: Ctx, body_in: ScheduleCreateBody) -> dict[str, Any]:
    """Convenience over ``POST /v1/workspaces/{ws}/items`` for a ``schedule`` item. Body: ``{"workspace_id", "name", "definition": {cron, timezone, target, catchup, ...}}``."""
    body = dump(body_in)
    ws = body.get("workspace_id")
    if not ws or not body.get("name"):
        raise ValidationFailed("workspace_id and name are required")
    principal.require("item.create", workspace_id=ws)
    from olcf.core.models import ItemCreate

    item = ctx.service("items").create(principal, ws, ItemCreate(type="schedule", name=str(body["name"]), definition=body.get("definition") or {}, description=str(body.get("description") or "")))
    t = _trig(ctx)
    t.reconcile_schedules()
    trg = next((r for r in t.list(ws, type_="schedule") if r["item_id"] == item["id"]), None)
    return _schedule_out(ctx, item, trg)


@schedules_router.get("/{item_id}")
def get_schedule(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    item = _item(ctx, principal, item_id, type_="schedule")
    t = _trig(ctx)
    trg = next((r for r in t.list(item["workspace_id"], type_="schedule") if r["item_id"] == item["id"]), None)
    return _schedule_out(ctx, item, trg)


@schedules_router.post("/{item_id}/enable")
def enable_schedule(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.update")
    t = _trig(ctx)
    t.set_enabled(principal, trg["id"], True)
    return get_schedule(item_id, principal, ctx)


@schedules_router.post("/{item_id}/disable")
def disable_schedule(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.update")
    t = _trig(ctx)
    t.set_enabled(principal, trg["id"], False)
    return get_schedule(item_id, principal, ctx)


@schedules_router.post("/{item_id}/pause")
def pause_schedule(item_id: str, principal: Authed, ctx: Ctx, body_in: PauseBody) -> dict[str, Any]:
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.update")
    _trig(ctx).pause(trg["id"], _until(dump(body_in)))
    return get_schedule(item_id, principal, ctx)


@schedules_router.post("/{item_id}/resume")
def resume_schedule(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.update")
    _trig(ctx).pause(trg["id"], None)
    return get_schedule(item_id, principal, ctx)


@schedules_router.post("/{item_id}/run-now", status_code=202)
def run_schedule_now(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run the bound pipeline / notebook right now as the caller (does not move the schedule's own cursor)."""
    _, trg = _schedule_trigger(ctx, principal, item_id, "pipeline.run")
    return _trig(ctx).run_now(principal, trg["id"])


@schedules_router.get("/{item_id}/next")
def schedule_next(item_id: str, principal: Authed, ctx: Ctx, count: int = Query(5, ge=1, le=100)) -> dict[str, Any]:
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.read")
    cursor = (trg.get("state") or {}).get("cursor")
    return {"items": _trig(ctx).preview(trg, count, from_iso(cursor) if cursor else None)}


@schedules_router.get("/{item_id}/runs")
def schedule_runs(item_id: str, principal: Authed, ctx: Ctx, limit: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
    """Fire history of the schedule including skipped (missed) fire times."""
    _, trg = _schedule_trigger(ctx, principal, item_id, "item.read")
    return {"items": _trig(ctx).runs(trg["id"], limit)}


# ---------------------------------------------------------------------------------------------------------------------------------------- public hooks
async def _read_capped(request: Request, cap: int) -> bytes:
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > cap:
        raise PayloadTooLarge(f"the request body is larger than {cap} bytes")
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > cap:
            raise PayloadTooLarge(f"the request body is larger than {cap} bytes")
    return bytes(buf)


@hooks_router.post("/pipelines/callbacks/{activity_run_id}/{token}", status_code=202)
async def pipeline_callback(activity_run_id: str, token: str, request: Request, ctx: Ctx) -> dict[str, Any]:
    """Callback URL of a ``webhook_wait`` activity. The token in the path is the credential; the body (JSON) completes or fails the activity."""
    raw = await _read_capped(request, 256 * 1024)
    return await run_in_threadpool(_svc(ctx).deliver_callback, activity_run_id, token, raw)


@hooks_router.post("/pipelines/{trigger_id}", status_code=202)
async def pipeline_webhook(trigger_id: str, request: Request, ctx: Ctx) -> dict[str, Any]:
    """Inbound webhook trigger. Authenticate with ``X-OLCF-Signature: sha256=<hmac of "<timestamp>.<body>">`` and ``X-OLCF-Timestamp`` (or a bearer token for token triggers).

    Answers 202 ``{status: accepted, run_id|job_id}``; 401 bad / missing signature or stale timestamp, 404 unknown trigger, 409 replayed delivery, 413 body too large,
    429 rate limited.
    """
    raw = await _read_capped(request, MAX_BODY_BYTES)
    headers = {k.lower(): v for k, v in request.headers.items()}
    return await run_in_threadpool(_trig(ctx).handle_webhook, trigger_id, headers, raw)
