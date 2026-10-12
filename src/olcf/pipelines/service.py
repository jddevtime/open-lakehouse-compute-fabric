# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``PipelineService`` (registered as ``ctx.service("pipelines")``): the authorised facade over the engine, the trigger service and the run read-models.

Everything the REST layer and other modules need goes through here so that authorisation, redaction and representation rules live in one place:

* run / re-run / resume / cancel (``pipeline.run`` on the pipeline item, ``item.read`` to look at a run),
* the *run graph*: the static activity graph of the run's definition snapshot merged with the activity rows (state, attempts, timings, iterations, job and log links),
* item hooks for the ``pipeline`` and ``schedule`` item types (validation on save, trigger synchronisation, cleanup on delete),
* redaction: inputs are stored redacted, outputs are redacted again on the way out (and withheld entirely for activities with ``policy.secureOutput``).
"""

from __future__ import annotations

import copy
import json
import logging
from typing import Any

from sqlalchemy import func, select, update

from olcf.core import refs
from olcf.core.errors import BadRequest, Conflict, NotFound, ValidationFailed
from olcf.core.redaction import redact_obj
from olcf.core.timeutil import from_iso, to_iso
from olcf.db.util import fetch_all, fetch_one, paginate
from olcf.pipelines import model as M
from olcf.pipelines.activities.base import Container
from olcf.pipelines.engine import RUN_TERMINAL, PipelineEngine, _dur
from olcf.pipelines.schema import activity_runs as A
from olcf.pipelines.schema import pipeline_runs as R

log = logging.getLogger("olcf.pipelines.service")

RUN_LIST_COLUMNS = [c for c in R.c if c.name not in ("definition",)]
ACTIVE_STATES = ("queued", "running", "cancel_requested")
#: ``GET /workspaces/{ws}/pipelines?include=definition`` stops embedding definitions once this many bytes were added to one response (the rest carry ``definition_omitted``).
LIST_DEFINITION_CAP_BYTES = 2 * 1024 * 1024


def walk_definition(definition: dict[str, Any], registry: Any) -> list[dict[str, Any]]:
    """Flatten a definition into ``[{id, name, type, parent, raw}]``; ``id`` is ``<container id>/<name>`` (iterations share their template's id)."""
    out: list[dict[str, Any]] = []

    def rec(acts: list[dict[str, Any]], parent: str | None) -> None:
        for a in acts or []:
            if not isinstance(a, dict) or not a.get("name"):
                continue
            nid = (parent + "/" if parent else "") + a["name"]
            out.append({"id": nid, "name": a["name"], "type": a.get("type"), "parent": parent, "raw": a})
            impl = registry.get(a.get("type", ""))
            if isinstance(impl, Container):
                for _k, inner in impl.scopes(a.get("typeProperties") or {}):
                    rec(inner or [], nid)

    rec(definition.get("activities") or [], None)
    return out


#: typeProperties keys that hold an item reference (id or name) per activity type; ``lakehouse`` / ``environment`` apply to every type.
_ACTIVITY_REF_KEYS = {"notebook": ("notebook",), "spark_job": ("sparkJob",), "child_pipeline": ("pipeline",)}
_COMMON_REF_KEYS = ("lakehouse", "environment")


def _remap_activity(act: dict[str, Any], id_map: dict[str, str]) -> None:
    props = act.get("typeProperties")
    if not isinstance(props, dict):
        return
    for key in (*_ACTIVITY_REF_KEYS.get(str(act.get("type")), ()), *_COMMON_REF_KEYS):
        if isinstance(props.get(key), str) and props[key] in id_map:
            props[key] = id_map[props[key]]
    for side in ("source", "sink"):
        sub = props.get(side)
        if isinstance(sub, dict) and isinstance(sub.get("lakehouse"), str) and sub["lakehouse"] in id_map:
            sub["lakehouse"] = id_map[sub["lakehouse"]]


def _walk_activities(node: Any, id_map: dict[str, str]) -> None:
    if isinstance(node, list):
        for x in node:
            _walk_activities(x, id_map)
    elif isinstance(node, dict):
        if "typeProperties" in node and "type" in node:
            _remap_activity(node, id_map)
        for v in node.values():
            _walk_activities(v, id_map)


def remap_pipeline_ids(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Workspace import / promotion: notebook / spark-job / child-pipeline / lakehouse / environment references of every activity (containers included)
    follow the imported items.  References by *name* are left alone (names survive an import), expressions are never rewritten."""
    out = copy.deepcopy(defn)
    _walk_activities(out.get("activities"), id_map)
    return out


def remap_schedule_ids(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Workspace import / promotion: ``target.ref`` follows the imported pipeline / notebook / spark job definition."""
    return refs.remap_paths(defn, id_map, ["target.ref"])


class PipelineService:
    """Authorised operations on pipelines, runs, schedules and triggers (see module docstring)."""

    def __init__(self, ctx: Any, engine: PipelineEngine, triggers: Any) -> None:
        self.ctx, self.db, self.engine, self.triggers = ctx, ctx.db, engine, triggers
        self.registry = engine.registry

    # ------------------------------------------------------------------------------------------------------------------------------ item hooks
    def validate_pipeline_definition(self, definition: dict[str, Any]) -> dict[str, Any]:
        """``validate`` hook of the ``pipeline`` item type: returns the normalized definition or raises with every problem."""
        if not isinstance(definition, dict):
            raise ValidationFailed("a pipeline definition must be an object")
        if not definition:
            # an empty item is allowed (created from the UI before the first edit); it simply cannot be run
            return definition
        res = M.validate_definition(definition, self.registry)
        res.raise_if_invalid()
        return res.definition

    def on_pipeline_deleted(self, item: dict[str, Any]) -> None:
        self.triggers.disable_for_target(item["id"], f"the pipeline '{item['name']}' was deleted")

    def install_hooks(self) -> None:
        reg = self.ctx.register_item_hooks
        reg("pipeline", validate=self.validate_pipeline_definition, on_delete=self.on_pipeline_deleted, remap_ids=remap_pipeline_ids)
        reg("schedule", validate=self.triggers.validate_schedule, on_create=self._schedule_created, on_delete=self.triggers.remove_schedule_item,
            remap_ids=remap_schedule_ids)

    def _schedule_created(self, item: dict[str, Any]) -> None:
        try:
            self.triggers.sync_schedule_item(item)
        except Exception:  # noqa: BLE001 - the reconcile loop retries; creating the item must not fail after it was committed
            log.exception("schedule item %s could not be synchronised yet", item.get("id"))

    # ------------------------------------------------------------------------------------------------------------------------------ validation / schema
    def validate(self, body: dict[str, Any]) -> dict[str, Any]:
        """Validate a pipeline document given as ``definition`` (object) or ``text`` (+ optional ``format`` json|yaml)."""
        if "text" in body and body["text"] is not None:
            try:
                defn = M.parse_definition_text(body["text"], body.get("format"))
            except ValidationFailed as exc:  # the endpoint reports problems in the body, it does not fail
                return {"valid": False, "errors": [{"path": "$", "message": exc.message, "code": "parse_error", "severity": "error"}], "warnings": [], "definition": None, "graph": None}
        else:
            defn = body.get("definition")
        if not isinstance(defn, dict):
            raise BadRequest("provide 'definition' (object) or 'text' (JSON or YAML)")
        return M.validate_definition(defn, self.registry).to_dict()

    def schema(self) -> dict[str, Any]:
        return M.build_schema(self.registry)

    def activity_types(self) -> list[dict[str, Any]]:
        return [a.describe() for a in self.registry.all()]

    # ------------------------------------------------------------------------------------------------------------------------------ lookups
    def pipeline_item(self, workspace_id: str, ref: str) -> dict[str, Any]:
        return self.ctx.service("items").resolve(workspace_id, ref, "pipeline")

    def _run_with_access(self, principal: Any, run_id: str, perm: str = "item.read") -> dict[str, Any]:
        run = self.engine.run_row(run_id)
        principal.require(perm, workspace_id=run["workspace_id"], item_id=run["pipeline_id"])
        return run

    # ------------------------------------------------------------------------------------------------------------------------------ run operations
    def run(self, principal: Any, workspace_id: str, ref: str, parameters: dict[str, Any] | None = None, *, idempotency_key: str | None = None,
            trigger: dict[str, Any] | None = None, queue_policy: str | None = None) -> dict[str, Any]:
        """Start a run (``trigger.type`` = manual by default). Returns the public run (state ``queued``; the dispatcher admits it)."""
        item = self.pipeline_item(workspace_id, ref)
        principal.require("pipeline.run", workspace_id=item["workspace_id"], item_id=item["id"])
        if parameters is not None and not isinstance(parameters, dict):
            raise ValidationFailed("parameters must be an object")
        trig = dict(trigger or {"type": "manual"})
        trig.setdefault("by", principal.name)
        key = f"api:{principal.id}:{idempotency_key}"[:191] if idempotency_key else None
        run = self.engine.create_run(principal, item, parameters or {}, trig, idempotency_key=key, queue_policy=queue_policy)
        self.ctx.audit.record(principal, "pipeline.run", resource_type="pipeline", resource_id=item["id"], workspace_id=item["workspace_id"], details={"run_id": run["id"]})
        return self.public_run(run)

    def get_run(self, principal: Any, run_id: str) -> dict[str, Any]:
        return self.public_run(self._run_with_access(principal, run_id))

    def cancel(self, principal: Any, run_id: str, reason: str | None = None) -> dict[str, Any]:
        run = self._run_with_access(principal, run_id, "pipeline.run")
        out = self.engine.cancel_run(run_id, (reason or f"cancelled by {principal.name}")[:500])
        self.ctx.audit.record(principal, "pipeline.run.cancel", resource_type="pipeline_run", resource_id=run_id, workspace_id=run["workspace_id"])
        return self.public_run(out)

    def resume(self, principal: Any, run_id: str, *, rerun: list[str] | None = None, parameters: dict[str, Any] | None = None, latest_definition: bool = False,
               reuse: bool = True, idempotency_key: str | None = None) -> dict[str, Any]:
        """Resume-from-failure (``reuse=True``: successful activities are not executed again) or a full re-run (``reuse=False``)."""
        run = self._run_with_access(principal, run_id, "pipeline.run")
        key = f"resume:{principal.id}:{idempotency_key}"[:191] if idempotency_key else None
        new = self.engine.resume_run(principal, run_id, rerun=rerun, parameters=parameters, latest_definition=latest_definition, reuse=reuse, idempotency_key=key)
        self.ctx.audit.record(principal, "pipeline.run.resume", resource_type="pipeline_run", resource_id=run_id, workspace_id=run["workspace_id"], details={"new_run_id": new["id"], "rerun": rerun or []})
        return self.public_run(new)

    def list_runs(self, principal: Any, *, workspace_id: str | None = None, pipeline_id: str | None = None, state: str | None = None, trigger_type: str | None = None,
                  parent_run_id: str | None = None, group_id: str | None = None, since: str | None = None, limit: int = 50, cursor: str | None = None) -> dict[str, Any]:
        visible = principal.visible_workspaces("item.read")
        stmt = select(*RUN_LIST_COLUMNS)
        if workspace_id:
            principal.require("item.read", workspace_id=workspace_id)
            stmt = stmt.where(R.c.workspace_id == workspace_id)
        elif visible is not None:
            stmt = stmt.where(R.c.workspace_id.in_(list(visible) or [""]))
        if pipeline_id:
            stmt = stmt.where(R.c.pipeline_id == pipeline_id)
        if state:
            states = [s.strip() for s in state.split(",") if s.strip()]
            stmt = stmt.where(R.c.state.in_(states))
        if trigger_type:
            stmt = stmt.where(R.c.trigger_type == trigger_type)
        if parent_run_id:
            stmt = stmt.where(R.c.parent_run_id == parent_run_id)
        if group_id:
            stmt = stmt.where(R.c.group_id == group_id)
        if since:
            stmt = stmt.where(R.c.created_at >= since)
        with self.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, R, limit=limit, cursor=cursor, descending=True)
        return {"items": [self.public_run(r) for r in rows], "next_cursor": nxt}

    def workspace_pipelines(self, principal: Any, workspace_id: str, *, q: str | None = None, include_definition: bool = False, limit: int = 50,
                            cursor: str | None = None) -> dict[str, Any]:
        """One row per pipeline of a workspace with what a list page needs: the last run (state, timings) and a summary of its triggers/schedules.

        ``include_definition`` embeds each definition the caller may read (``item.read`` on that pipeline) until ``LIST_DEFINITION_CAP_BYTES`` is used up; the rest get
        ``definition_omitted: "size"`` and are fetched with ``GET /pipelines/{id}/definition``. Two queries (runs, triggers) serve the whole page, not one per pipeline.
        """
        principal.require("item.read", workspace_id=workspace_id)
        rows, nxt = self.ctx.service("items").list(workspace_id, type_="pipeline", q=q, limit=limit, cursor=cursor)
        rows = [r for r in rows if principal.can("item.read", workspace_id=workspace_id, item_id=r["id"])]
        ids = [r["id"] for r in rows]
        last: dict[str, dict[str, Any]] = {}
        if ids:
            newest = select(R.c.pipeline_id, func.max(R.c.created_at).label("m")).where(R.c.pipeline_id.in_(ids)).group_by(R.c.pipeline_id).subquery()
            with self.db.connect() as conn:
                for run in fetch_all(conn, select(*RUN_LIST_COLUMNS).join(newest, (R.c.pipeline_id == newest.c.pipeline_id) & (R.c.created_at == newest.c.m))):
                    last.setdefault(run["pipeline_id"], run)
        trig: dict[str, list[dict[str, Any]]] = {}
        if ids:
            idset = set(ids)
            for t in self.triggers.list(workspace_id, limit=1000):
                if t.get("target_id") in idset:
                    trig.setdefault(t["target_id"], []).append(t)
        used = 0
        items = []
        for r in rows:
            run = last.get(r["id"])
            ts = trig.get(r["id"], [])
            out: dict[str, Any] = {k: r[k] for k in ("id", "workspace_id", "name", "folder", "description", "tags", "version", "etag", "created_at", "updated_at")}
            out["last_run"] = None if run is None else {"id": run["id"], "state": run["state"], "state_reason": run.get("state_reason"), "trigger_type": run.get("trigger_type"),
                                                       "created_at": run.get("created_at"), "started_at": run.get("started_at"), "finished_at": run.get("finished_at"),
                                                       "duration_seconds": _dur(run), "pipeline_version": run.get("pipeline_version")}
            out["triggers"] = self._trigger_summary(ts)
            if include_definition:
                size = len(json.dumps(r.get("definition") or {}, default=str))
                if used + size > LIST_DEFINITION_CAP_BYTES:
                    out["definition"], out["definition_omitted"] = None, "size"
                else:
                    used += size
                    out["definition"] = r.get("definition") or {}
            items.append(out)
        return {"items": items, "next_cursor": nxt}

    def _trigger_summary(self, triggers: list[dict[str, Any]]) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        for t in triggers:
            by_type[t["type"]] = by_type.get(t["type"], 0) + 1
        nxt = None
        for t in triggers:
            if t["type"] == "schedule" and t.get("enabled"):
                try:
                    first = self.triggers.preview(t, 1)
                except Exception as exc:  # noqa: BLE001 - a broken cron must not break the list
                    log.debug("no next fire time for trigger %s: %s", t.get("id"), exc)
                    continue
                if first and (nxt is None or first[0]["utc"] < nxt):
                    nxt = first[0]["utc"]
        return {"total": len(triggers), "enabled": sum(1 for t in triggers if t.get("enabled")), "by_type": by_type, "next_fire": nxt}

    # ------------------------------------------------------------------------------------------------------------------------------ representations
    @staticmethod
    def public_run(row: dict[str, Any]) -> dict[str, Any]:
        out = {k: row.get(k) for k in ("id", "workspace_id", "pipeline_id", "pipeline_name", "pipeline_version", "state", "state_reason", "trigger_type", "principal_id", "parent_run_id",
                                        "parent_activity_id", "root_run_id", "depth", "resumed_from", "group_id", "queue_policy", "concurrency", "correlation_id", "created_at", "started_at",
                                        "finished_at", "updated_at", "cancel_requested_at", "cancel_reason", "def_hash")}
        trig = dict(row.get("trigger") or {})
        trig.pop("principal_scopes", None)
        trig.pop("principal_credential", None)
        out["trigger"] = redact_obj(trig)
        out["parameters"] = redact_obj(row.get("parameters") or {})
        out["variables"] = redact_obj(row.get("variables") or {})
        out["outputs"] = redact_obj(row.get("outputs")) if row.get("outputs") is not None else None
        out["error"] = redact_obj(row.get("error")) if row.get("error") is not None else None
        out["duration_seconds"] = _dur(row)
        out["finished"] = row.get("state") in RUN_TERMINAL
        out["links"] = {"self": f"/v1/pipeline-runs/{row['id']}", "graph": f"/v1/pipeline-runs/{row['id']}/graph", "activities": f"/v1/pipeline-runs/{row['id']}/activities"}
        return out

    def _secure_outputs(self, run_id: str) -> set[str]:
        with self.db.connect() as conn:
            defn = conn.execute(select(R.c.definition).where(R.c.id == run_id)).scalar() or {}
        return {n["name"] for n in walk_definition(defn, self.registry) if (n["raw"].get("policy") or {}).get("secureOutput")}

    def public_activity(self, row: dict[str, Any], *, secure: set[str] | None = None, include_io: bool = True, expose_state: bool = False) -> dict[str, Any]:
        out = {k: row.get(k) for k in ("id", "run_id", "parent_id", "iteration", "name", "type", "state", "attempt", "max_attempts", "attempts", "job_ids", "reused_from", "created_at",
                                        "started_at", "finished_at", "duration_ms", "next_attempt_at", "timeout_at", "cancel_requested")}
        if include_io:
            out["input"] = row.get("input")
            if secure and row["name"] in secure:
                out["output"] = {"secure": True} if row.get("output") is not None else None
            else:
                out["output"] = redact_obj(row.get("output")) if row.get("output") is not None else None
        out["error"] = redact_obj(row.get("error")) if row.get("error") is not None else None
        out["links"] = {"jobs": [f"/v1/jobs/{j}" for j in row.get("job_ids") or []], "logs": [f"/v1/jobs/{j}/logs" for j in row.get("job_ids") or []]}
        if expose_state and row.get("state") in ("running", "retry_wait", "ready"):
            impl = self.registry.get(str(row.get("type")))
            if impl is not None:
                out.update(impl.public_state(row.get("state_data") or {}))
        return out

    def activities(self, principal: Any, run_id: str, *, include_io: bool = False) -> list[dict[str, Any]]:
        run = self._run_with_access(principal, run_id)
        secure = self._secure_outputs(run_id)
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(A).where(A.c.run_id == run_id).order_by(A.c.created_at.asc(), A.c.id.asc()))
        # credentials-in-URLs (webhook_wait callbacks) are only shown to those who may run pipelines of the workspace
        expose = principal.can("pipeline.run", workspace_id=run["workspace_id"])
        return [self.public_activity(r, secure=secure, include_io=include_io, expose_state=expose) for r in rows]

    def activity(self, principal: Any, run_id: str, activity_id: str) -> dict[str, Any]:
        run = self._run_with_access(principal, run_id)
        with self.db.connect() as conn:
            row = fetch_one(conn, select(A).where(A.c.id == activity_id, A.c.run_id == run_id))
        if not row:
            raise NotFound(f"activity run {activity_id} not found in {run_id}")
        return self.public_activity(row, secure=self._secure_outputs(run_id), expose_state=principal.can("pipeline.run", workspace_id=run["workspace_id"]))

    def children(self, principal: Any, run_id: str) -> list[dict[str, Any]]:
        self._run_with_access(principal, run_id)
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(*RUN_LIST_COLUMNS).where(R.c.parent_run_id == run_id).order_by(R.c.created_at.asc()))
        return [self.public_run(r) for r in rows]

    # ------------------------------------------------------------------------------------------------------------------------------ run graph
    def graph(self, principal: Any, run_id: str, *, include_io: bool = False) -> dict[str, Any]:
        """Run graph with timings: static nodes/edges from the definition snapshot merged with the activity rows.

        Each node carries an aggregated ``state`` (``not_started`` until a row exists), the number of ``instances`` (for_each iterations / retried containers), per-instance
        rows with attempts, timings, job ids and ``logs`` links, and the ``timeline`` lists every executed instance ordered by start time.
        """
        run = self._run_with_access(principal, run_id)
        with self.db.connect() as conn:
            defn = conn.execute(select(R.c.definition).where(R.c.id == run_id)).scalar() or {}
            rows = fetch_all(conn, select(A).where(A.c.run_id == run_id).order_by(A.c.created_at.asc(), A.c.id.asc()))
            kids = fetch_all(conn, select(R.c.id, R.c.parent_activity_id, R.c.state, R.c.pipeline_name).where(R.c.parent_run_id == run_id).order_by(R.c.created_at.asc()))
        children_of: dict[str, list[dict[str, Any]]] = {}
        for k in kids:
            children_of.setdefault(str(k["parent_activity_id"]), []).append({"run_id": k["id"], "pipeline": k["pipeline_name"], "state": k["state"]})
        nodes = walk_definition(defn, self.registry)
        secure = {n["name"] for n in nodes if (n["raw"].get("policy") or {}).get("secureOutput")}
        by_id = {r["id"]: r for r in rows}
        node_of: dict[str, str] = {}

        def node_id(row: dict[str, Any]) -> str:
            hit = node_of.get(row["id"])
            if hit is not None:
                return hit
            parent = by_id.get(row["parent_id"]) if row.get("parent_id") else None
            nid = (node_id(parent) + "/" if parent else "") + row["name"]
            node_of[row["id"]] = nid
            return nid

        instances: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            instances.setdefault(node_id(r), []).append(r)
        out_nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        for n in nodes:
            inst = instances.get(n["id"], [])
            pub = [self.public_activity(r, secure=secure, include_io=include_io) for r in inst]
            starts = [r["started_at"] for r in inst if r.get("started_at")]
            ends = [r["finished_at"] for r in inst if r.get("finished_at")]
            terminal = all(r["state"] in ("succeeded", "failed", "skipped", "cancelled") for r in inst) if inst else False
            out_nodes.append({
                "id": n["id"], "name": n["name"], "type": n["type"], "parent": n["parent"], "state": _aggregate([r["state"] for r in inst]),
                "instances": len(inst), "attempts": sum(int(r.get("attempt") or 0) for r in inst), "started_at": min(starts) if starts else None,
                "finished_at": (max(ends) if terminal and ends else None), "duration_ms": _span_ms(min(starts), max(ends)) if starts and ends and terminal else None,
                "rows": pub, "reused": any(r.get("reused_from") for r in inst), "job_ids": [j for r in inst for j in (r.get("job_ids") or [])],
                "description": n["raw"].get("description"), "child_runs": [c for r in inst for c in children_of.get(r["id"], [])],
            })
            for dep in M.normalize_dependencies(n["raw"].get("dependsOn")):
                edges.append({"from": (n["parent"] + "/" if n["parent"] else "") + str(dep["activity"]), "to": n["id"], "conditions": dep["conditions"]})
        timeline = sorted(({"node": node_id(r), "id": r["id"], "iteration": r.get("iteration"), "state": r["state"], "started_at": r.get("started_at"), "finished_at": r.get("finished_at"),
                            "duration_ms": r.get("duration_ms")} for r in rows if r.get("started_at")), key=lambda x: (x["started_at"], x["id"]))
        counts: dict[str, int] = {}
        for r in rows:
            counts[r["state"]] = counts.get(r["state"], 0) + 1
        return {"run": self.public_run(run), "nodes": out_nodes, "edges": edges, "timeline": timeline, "counts": counts,
                "critical_path_ms": _critical_path(out_nodes, edges)}

    # ------------------------------------------------------------------------------------------------------------------------------ webhook_wait callback
    def deliver_callback(self, activity_run_id: str, token: str, raw: bytes) -> dict[str, Any]:
        """Complete a ``webhook_wait`` activity from its callback URL. The (unguessable) token in the URL *is* the credential; unknown id and wrong token are indistinguishable."""
        from olcf.pipelines.activities.base import ActivityError
        from olcf.pipelines.activities.control import callback_body, verify_callback_token

        self.ctx.limiter.check(f"cb:{activity_run_id}", limit=30, window=60.0)
        try:
            body = callback_body(raw)
        except ActivityError as exc:
            raise BadRequest(str(exc)) from None
        with self.db.begin() as conn:
            row = fetch_one(conn, select(A.c.id, A.c.run_id, A.c.state, A.c.state_data, A.c.signal, A.c.type).where(A.c.id == activity_run_id).with_for_update())
            if not row or row["type"] != "webhook_wait" or not verify_callback_token(row["state_data"] or {}, token):
                raise NotFound("callback not found")
            if row["signal"] is not None and (row["signal"] or {}).get("body") == body:
                return {"status": "accepted", "duplicate": True}          # senders retry: an identical delivery is acknowledged, even after the activity finished
            if row["state"] not in ("running", "retry_wait", "ready"):
                raise Conflict(f"the activity already finished ({row['state']})")
            if row["signal"] is not None:
                raise Conflict("a different callback was already received for this activity")
            conn.execute(update(A).where(A.c.id == activity_run_id).values(signal={"body": body, "at": to_iso(self.engine.now())}, updated_at=self.engine.now_iso()))
        self.engine.wake()
        return {"status": "accepted"}

    # ------------------------------------------------------------------------------------------------------------------------------ stats
    def stats(self) -> dict[str, Any]:
        """Aggregate counters for health checks and metrics."""
        with self.db.connect() as conn:
            rows = conn.execute(select(R.c.state, func.count()).group_by(R.c.state)).all()
        return {"runs_by_state": {r[0]: int(r[1]) for r in rows}}


def _aggregate(states: list[str]) -> str:
    if not states:
        return "not_started"
    s = set(states)
    for st in ("running", "retry_wait", "ready"):
        if st in s:
            return "running" if st != "ready" else "ready"
    for st in ("failed", "cancelled"):
        if st in s:
            return st
    if s == {"skipped"}:
        return "skipped"
    return "succeeded"


def _span_ms(a: str, b: str) -> int | None:
    try:
        return int((from_iso(b) - from_iso(a)).total_seconds() * 1000)
    except Exception:  # noqa: BLE001
        return None


def _critical_path(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> int | None:
    """Longest chain of (sibling) dependencies by duration - a hint about where the run spent its time."""
    dur = {n["id"]: int(n["duration_ms"] or 0) for n in nodes}
    preds: dict[str, list[str]] = {}
    for e in edges:
        preds.setdefault(e["to"], []).append(e["from"])
    memo: dict[str, int] = {}

    def best(nid: str, depth: int = 0) -> int:
        if nid in memo:
            return memo[nid]
        if depth > 600:
            return dur.get(nid, 0)
        memo[nid] = dur.get(nid, 0)  # guards against any cycle
        memo[nid] = dur.get(nid, 0) + max((best(p, depth + 1) for p in preds.get(nid, [])), default=0)
        return memo[nid]

    if not nodes:
        return None
    return max(best(n["id"]) for n in nodes)
