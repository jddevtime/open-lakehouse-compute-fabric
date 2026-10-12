# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""ORCH module: pipelines, schedules and triggers, outbound webhooks.

Services registered (CONTRACTS section 4.1)
    ``pipelines``  :class:`~olcf.pipelines.service.PipelineService` (``run / get_run / cancel / resume / list_runs / graph / validate / schema``; ``.engine``, ``.triggers``, ``.registry``)
    ``triggers``   :class:`~olcf.pipelines.triggers.TriggerService`
    ``webhooks``   :class:`~olcf.pipelines.webhooks.WebhookService`

Item types served: ``pipeline`` (definition validated on save, triggers removed on delete) and ``schedule`` (cron definition validated on save, kept in sync with its trigger).

Routes (under ``/v1``): ``/pipelines``, ``/pipeline-runs``, ``/triggers``, ``/schedules``, ``/webhooks`` and the *public* inbound endpoints ``/hooks/pipelines/{trigger_id}`` and
``/hooks/pipelines/callbacks/{activity_run_id}/{token}`` (they authenticate with a signature / token instead of a bearer credential).

Background work (stopped by ``stop()``; every loop does its work on the leader only, so a replica that takes over continues from the database state)
    * the pipeline dispatcher (``PipelineEngine``),
    * schedule / storage-arrival / dependency trigger loops,
    * the outbound webhook dispatcher and the audit-to-event bridge (``security.*`` / ``fabric.*`` events).

Health check ``pipelines-engine``: dispatcher liveness on this replica; deep mode validates a definition, evaluates the expression engine, round-trips a run read and checks the
trigger, webhook and activity-plugin state.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import select

from olcf.core.context import AppContext, Module
from olcf.core.errors import BadRequest
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.timeutil import from_iso
from olcf.db.util import fetch_all
from olcf.observability import correlation
from olcf.pipelines import api, ssrf, webhooks_api
from olcf.pipelines import model as M
from olcf.pipelines.activities import build_registry
from olcf.pipelines.common import setting
from olcf.pipelines.engine import PipelineEngine
from olcf.pipelines.expressions import EvalContext, evaluate_value
from olcf.pipelines.schema import activity_runs as A
from olcf.pipelines.service import PipelineService
from olcf.pipelines.triggers import TriggerLoops, TriggerService
from olcf.pipelines.webhooks import AuditEventBridge, WebhookService

log = logging.getLogger("olcf.pipelines")

_TRACE_STATES = {"succeeded": False, "failed": True, "cancelled": False, "skipped": False}


def _int_setting(lo: int, hi: int) -> Any:
    def v(x: Any) -> int:
        if isinstance(x, bool) or not isinstance(x, (int, float)) or int(x) != x or not lo <= int(x) <= hi:
            raise BadRequest(f"must be an integer between {lo} and {hi}")
        return int(x)

    return v


class PipelinesModule(Module):
    """Module that registers the pipeline engine, triggers, webhooks and their routes."""
    name = "pipelines"

    def __init__(self, ctx: AppContext, engine: PipelineEngine, triggers: TriggerService, service: PipelineService, hooks: WebhookService, bridge: AuditEventBridge) -> None:
        super().__init__()
        self.ctx, self.engine, self.triggers, self.service, self.webhooks, self.bridge = ctx, engine, triggers, service, hooks, bridge
        self.loops = TriggerLoops(ctx, triggers)
        self.routers = [(api.pipelines_router, "/pipelines"), (api.workspace_pipelines_router, "/workspaces"), (api.runs_router, "/pipeline-runs"), (api.triggers_router, "/triggers"), (api.schedules_router, "/schedules"),
                        (webhooks_api.router, "/webhooks")]
        #: mounted under /v1 *without* bearer authentication (the endpoints verify signatures / tokens themselves)
        self.v1_public = [(api.hooks_router, "/hooks")]
        self._started = False

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.engine.hooks.append(self._trace_run)
        self.engine.start()
        self.loops.start()
        self.webhooks.start()
        self.bridge.start()

    def stop(self) -> None:
        """Stop the loops first (no new work), then the dispatcher; in-flight attempts keep their lease and are adopted after a restart / failover."""
        for step, fn in (("triggers", self.loops.stop), ("webhooks", self.webhooks.stop), ("audit bridge", self.bridge.stop), ("engine", self.engine.stop)):
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.exception("pipelines %s failed to stop", step)
        try:
            self.engine.hooks.remove(self._trace_run)
        except ValueError:
            pass
        self._started = False

    # ------------------------------------------------------------------------------------------------------------------------------ tracing
    def _trace_run(self, run: dict[str, Any], state: str) -> None:
        """Engine hook: export the finished run as a trace (run span + one span per activity attempt row) when OTLP trace export is on."""
        tracer = self.ctx.maybe_service("tracer")
        if tracer is None or not tracer.enabled or not run.get("started_at"):
            return
        trace_id, root = correlation.new_trace_id(), correlation.new_span_id()
        end = from_iso(run["finished_at"]).timestamp() if run.get("finished_at") else time.time()
        tracer.add(name=f"pipeline {run['pipeline_name']}", trace_id=trace_id, span_id=root, parent_id=None, start=from_iso(run["started_at"]).timestamp(), end=end,
                   attributes={"olcf.run_id": run["id"], "olcf.pipeline": run["pipeline_name"], "olcf.workspace_id": run["workspace_id"], "olcf.trigger": run.get("trigger_type"),
                               "olcf.state": state}, error=(run.get("state_reason") or state) if _TRACE_STATES.get(state) else None)
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(A.c.id, A.c.name, A.c.type, A.c.state, A.c.attempt, A.c.started_at, A.c.finished_at, A.c.error).where(A.c.run_id == run["id"], A.c.started_at.is_not(None)).limit(2000))
        for r in rows:
            try:
                s = from_iso(r["started_at"]).timestamp()
                e = from_iso(r["finished_at"]).timestamp() if r["finished_at"] else end
                tracer.add(name=f"activity {r['name']}", trace_id=trace_id, span_id=correlation.new_span_id(), parent_id=root, start=s, end=e,
                           attributes={"olcf.activity_id": r["id"], "olcf.activity_type": r["type"], "olcf.attempt": r["attempt"], "olcf.state": r["state"]},
                           error=str((r["error"] or {}).get("message") or r["state"])[:300] if r["state"] == "failed" else None)
            except Exception:  # noqa: BLE001
                log.debug("span for activity %s skipped", r.get("id"), exc_info=True)

    # ------------------------------------------------------------------------------------------------------------------------------ health
    def register_health(self) -> None:
        self.ctx.health.register(HealthCheck("pipelines-engine", "control-plane", "Pipeline dispatcher, triggers and webhook delivery", self._quick, self._deep, ttl=10))

    def _quick(self) -> ComponentHealth:
        e = self.engine
        leader = bool(self.ctx.leader.is_leader())
        thread_alive = bool(e._thread and e._thread.is_alive())  # noqa: SLF001
        labels: dict[str, Any] = {"leader": leader, "activity_plugins": list(e.registry.plugins), "rejected_plugins": len(e.registry.rejected)}
        if e.dead or not thread_alive:
            return ComponentHealth("pipelines-engine", Status.DOWN, "the pipeline dispatcher is not running on this replica", category="control-plane", labels=labels)
        age = None if e.last_loop_at is None else time.monotonic() - e.last_loop_at
        if e.last_error:
            return ComponentHealth("pipelines-engine", Status.DEGRADED, f"the last dispatcher iteration failed: {e.last_error}", category="control-plane", labels=labels)
        if age is not None and age > 30:
            return ComponentHealth("pipelines-engine", Status.DEGRADED, f"the dispatcher has not completed an iteration for {age:.0f} s", category="control-plane", labels=labels)
        try:
            st = self.service.stats()["runs_by_state"]
        except Exception as exc:  # noqa: BLE001
            return ComponentHealth("pipelines-engine", Status.DOWN, f"cannot read pipeline runs: {type(exc).__name__}: {exc}", category="control-plane", labels=labels)
        labels["runs"] = st
        role = "leader (executing runs)" if leader else "standby (the leader executes runs)"
        return ComponentHealth("pipelines-engine", Status.OK, f"dispatcher running; {role}; {st.get('running', 0)} running, {st.get('queued', 0)} queued", category="control-plane", labels=labels)

    def _deep(self) -> ComponentHealth:
        steps: list[dict[str, Any]] = []
        status = Status.OK

        def step(name: str, fn: Any) -> None:
            nonlocal status
            t0 = time.perf_counter()
            try:
                detail = fn()
                steps.append({"name": name, "ok": True, "detail": detail, "ms": round((time.perf_counter() - t0) * 1000, 1)})
            except Exception as exc:  # noqa: BLE001
                status = Status.DOWN
                steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}", "ms": round((time.perf_counter() - t0) * 1000, 1)})

        def validate() -> str:
            res = M.validate_definition({"schema": M.SCHEMA_ID, "parameters": {"n": {"type": "int", "default": 2}},
                                         "activities": [{"name": "w", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@pipeline().parameters.n"}},
                                                        {"name": "v", "type": "set_variable", "dependsOn": [{"activity": "w", "conditions": ["succeeded"]}],
                                                         "typeProperties": {"variable": "x", "value": "@activity('w').output.waited"}}], "variables": {"x": {"type": "string"}}},
                                        self.engine.registry)
            if not res.ok:
                raise RuntimeError("; ".join(i.message for i in res.errors[:3]))
            return f"validator accepted a 2-activity definition; {len(self.engine.registry.all())} activity types"

        def expression() -> str:
            got = evaluate_value("@add(mul(2, 3), length(createArray(1, 2)))", EvalContext())
            if got != 8:
                raise RuntimeError(f"expression engine returned {got!r}, expected 8")
            return "expression engine evaluated a nested call"

        def db() -> str:
            st = self.service.stats()["runs_by_state"]
            return f"pipeline_runs readable ({sum(st.values())} runs)"

        def triggers() -> str:
            s = self.triggers.stats() if hasattr(self.triggers, "stats") else {}
            return "trigger table readable" + (f": {s}" if s else "")

        def hooks() -> str:
            s = self.webhooks.stats()
            return f"webhook tables readable: {s}"

        step("definition validation", validate)
        step("expression engine", expression)
        step("run store", db)
        step("trigger store", triggers)
        step("webhook store", hooks)
        if self.engine.registry.rejected:
            steps.append({"name": "activity plugins", "ok": True, "detail": f"{len(self.engine.registry.rejected)} plugin activity type(s) rejected: "
                                                                          + "; ".join(f"{r['type']} ({r['reason']})" for r in self.engine.registry.rejected[:3]), "ms": 0})
        return ComponentHealth("pipelines-engine", status, "deep check " + ("passed" if status is Status.OK else "failed"), category="control-plane", steps=steps, mode="deep")


def register_settings(ctx: AppContext) -> None:
    st = ctx.service("settings")
    st.register("pipelines.max_active_runs", description="Maximum pipeline runs executing at the same time across the control plane (queued runs wait)", default=500,
                validator=_int_setting(1, 100000))
    st.register("pipelines.min_schedule_interval_seconds", description="Smallest allowed interval between two fire times of a cron schedule", default=60, validator=_int_setting(1, 86400))


def install(ctx: AppContext) -> PipelinesModule:
    ssrf.register_settings(ctx)
    register_settings(ctx)
    registry = build_registry()
    engine = PipelineEngine(ctx, registry, max_active_runs=int(setting(ctx, "pipelines.max_active_runs", 500)))
    triggers = TriggerService(ctx, engine)
    service = PipelineService(ctx, engine, triggers)
    hooks = WebhookService(ctx)
    hooks.register_settings()
    bridge = AuditEventBridge(ctx)
    ctx.register_service("pipelines", service)
    ctx.register_service("triggers", triggers)
    ctx.register_service("webhooks", hooks)
    service.install_hooks()
    mod = PipelinesModule(ctx, engine, triggers, service, hooks, bridge)
    mod.register_health()
    return mod
