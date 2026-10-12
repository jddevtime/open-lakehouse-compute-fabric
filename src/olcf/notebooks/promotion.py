# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Deployment promotion between workspaces (dev -> test -> prod): ``ctx.service("deployments")``.

A *deployment pipeline* names an ordered list of stages, each bound to one workspace, plus rules::

    {"stages": [{"stage": "dev", "workspace_id": "wks_..."}, {"stage": "test", ...}, {"stage": "prod", ...}],
     "rules": {"connections": {"test": {"dev-warehouse": "test-warehouse"}, "prod": {"dev-warehouse": "prod-warehouse"}},
               "variable_stage": {"test": "test", "prod": "prod"},     # which variable-set stage applies in the target (default: the stage name)
               "connection_mode": "map",                               # map (default): never create connections; copy: also create missing ones (metadata only)
               "include_types": [], "exclude_types": [], "delete_missing": false}}

``deploy`` copies the items of the source stage's workspace into the next stage's workspace through the same unit layer as Git sync
(:mod:`olcf.notebooks.treeapply`): items are matched by identity (``logicalId``) and then by name, created or updated through ``ItemService`` (so each
promotion is a new item *version* that can be restored), references to source items are rewritten to the target items, **connection references are remapped
by the rules** (a reference that has no counterpart in the target is an error, never silently kept), and the target workspace's active stage is set so that the
variable-set **stage overrides** (``dev`` / ``test`` / ``prod`` values) apply there.  Secrets are never copied: a variable of type ``secret`` is a reference, and a
promoted connection is created without its secret.  ``dry_run`` returns the plan without writing anything.  Every deployment is recorded in ``deployments``.
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from olcf.core.context import AppContext
from olcf.core.errors import Conflict, NotFound, OlcfError, ValidationFailed
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, get_by_id, insert_row, new_etag, paginate
from olcf.notebooks import treeapply, workspace_io
from olcf.notebooks import variables as var
from olcf.notebooks.schema import deployment_pipelines as DP
from olcf.notebooks.schema import deployments as DEP
from olcf.security.rbac import Principal

_STAGE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$")
RULE_KEYS = {"connections", "variable_stage", "connection_mode", "include_types", "exclude_types", "delete_missing"}


class StageIn(BaseModel):
    """One stage of a deployment pipeline in a request: stage name and workspace id."""
    model_config = ConfigDict(extra="forbid")
    stage: str
    workspace_id: str


class PipelineCreate(BaseModel):
    """Request body for creating a deployment pipeline: name, description and between two and eight stages."""
    model_config = ConfigDict(extra="forbid")
    name: str
    description: str = ""
    stages: list[StageIn] = Field(min_length=2, max_length=8)
    rules: dict[str, Any] = Field(default_factory=dict)


class PipelineUpdate(BaseModel):
    """Request body for changing a deployment pipeline's description, stages or rules."""
    model_config = ConfigDict(extra="forbid")
    description: str | None = None
    stages: list[StageIn] | None = Field(None, min_length=2, max_length=8)
    rules: dict[str, Any] | None = None


class DeployRequest(BaseModel):
    """Request body for promoting items from one stage to the next; by default from the first stage, all items."""
    model_config = ConfigDict(extra="forbid")
    from_stage: str | None = Field(None, description="default: the first stage")
    to_stage: str | None = Field(None, description="default: the stage after from_stage")
    items: list[str] | None = Field(None, description="promote only these items (id or name); default all")
    dry_run: bool = False
    allow_skip: bool = Field(False, description="allow promoting to a stage that is not the next one")
    comment: str = Field("", max_length=500)


Rules = dict[str, Any]


def normalise_rules(rules: Any, stages: list[str]) -> Rules:
    """Validate pipeline rules (unknown keys are rejected)."""
    if not isinstance(rules, dict):
        raise ValidationFailed("rules must be an object")
    bad = set(rules) - RULE_KEYS
    if bad:
        raise ValidationFailed(f"unknown rule keys {sorted(bad)}", details={"known": sorted(RULE_KEYS)})
    out: Rules = {}
    conns = rules.get("connections", {})
    if not isinstance(conns, dict):
        raise ValidationFailed("rules.connections must be {stage: {source: target}}")
    nc: dict[str, dict[str, str]] = {}
    for stg, mp in conns.items():
        if stg not in stages:
            raise ValidationFailed(f"rules.connections refers to unknown stage '{stg}'", details={"stages": stages})
        if not isinstance(mp, dict) or not all(isinstance(k, str) and isinstance(v, str) and k and v for k, v in mp.items()):
            raise ValidationFailed(f"rules.connections.{stg} must map connection names / ids to connection names")
        nc[stg] = dict(mp)
    if nc:
        out["connections"] = nc
    vs = rules.get("variable_stage", {})
    if not isinstance(vs, dict) or not all(k in stages and isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,32}", v) for k, v in vs.items()):
        raise ValidationFailed("rules.variable_stage must be {pipeline stage: variable-set stage}")
    if vs:
        out["variable_stage"] = dict(vs)
    mode = rules.get("connection_mode", "map")
    if mode not in ("map", "copy"):
        raise ValidationFailed("rules.connection_mode must be 'map' or 'copy'")
    out["connection_mode"] = mode
    for k in ("include_types", "exclude_types"):
        v = rules.get(k, [])
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            raise ValidationFailed(f"rules.{k} must be a list of item types")
        if v:
            out[k] = list(v)
    out["delete_missing"] = bool(rules.get("delete_missing", False))
    return out


class DeploymentService:
    """Deployment pipelines and the ``deploy`` operation (see the module docstring)."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    # ------------------------------------------------------------------ pipelines
    @staticmethod
    def public(r: dict[str, Any]) -> dict[str, Any]:
        return {k: r[k] for k in ("id", "name", "description", "stages", "rules", "owner_id", "etag", "created_at", "updated_at")}

    def get(self, principal: Principal, pid: str, perm: str = "workspace.read") -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = get_by_id(conn, DP, pid) if pid.startswith("dpl_") else fetch_one(conn, select(DP).where(DP.c.name == pid, DP.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"deployment pipeline '{pid}' not found")
        if not any(principal.can(perm, workspace_id=s["workspace_id"]) for s in row["stages"]):
            raise NotFound(f"deployment pipeline '{pid}' not found")  # do not reveal pipelines of workspaces the caller cannot see
        return row

    def _stages(self, principal: Principal, stages: list[StageIn]) -> list[dict[str, str]]:
        names = [s.stage for s in stages]
        if len(set(names)) != len(names) or not all(_STAGE.match(n) for n in names):
            raise ValidationFailed("stage names must be unique and match [a-z][a-z0-9_-]{0,31}")
        wss = self.ctx.service("workspaces")
        out = []
        for s in stages:
            ws = wss.resolve(s.workspace_id)
            principal.require("workspace.update", workspace_id=ws["id"])
            out.append({"stage": s.stage, "workspace_id": ws["id"]})
        if len({s["workspace_id"] for s in out}) != len(out):
            raise ValidationFailed("every stage needs its own workspace")
        return out

    def create_pipeline(self, principal: Principal, body: PipelineCreate) -> dict[str, Any]:
        if not _NAME.match(body.name):
            raise ValidationFailed("invalid pipeline name")
        stages = self._stages(principal, body.stages)
        rules = normalise_rules(body.rules, [s["stage"] for s in stages])
        now = now_iso()
        row = {"id": new_id("dpl"), "name": body.name, "description": body.description, "stages": stages, "rules": rules, "owner_id": principal.id, "etag": new_etag(),
               "created_at": now, "updated_at": now, "deleted_at": None}
        with self.ctx.db.begin() as conn:
            if fetch_one(conn, select(DP.c.id).where(DP.c.name == body.name, DP.c.deleted_at.is_(None))):
                raise Conflict(f"deployment pipeline '{body.name}' already exists")
            insert_row(conn, DP, row)
        self.ctx.audit.record(principal, "deployment_pipeline.created", resource_type="deployment_pipeline", resource_id=row["id"], details={"name": body.name})
        return row

    def update_pipeline(self, principal: Principal, pid: str, body: PipelineUpdate, if_match: str | None) -> dict[str, Any]:
        from olcf.api.deps import check_if_match

        row = self.get(principal, pid, "workspace.update")
        check_if_match(if_match, row["etag"])
        vals: dict[str, Any] = {}
        stages = row["stages"]
        if body.stages is not None:
            stages = self._stages(principal, body.stages)
            vals["stages"] = stages
        if body.rules is not None or body.stages is not None:
            vals["rules"] = normalise_rules(body.rules if body.rules is not None else row["rules"], [s["stage"] for s in stages])
        if body.description is not None:
            vals["description"] = body.description
        if not vals:
            return row
        with self.ctx.db.begin() as conn:
            conn.execute(update(DP).where(DP.c.id == row["id"]).values(**vals, etag=new_etag(), updated_at=now_iso()))
            new = get_by_id(conn, DP, row["id"])
        self.ctx.audit.record(principal, "deployment_pipeline.updated", resource_type="deployment_pipeline", resource_id=row["id"], details={"fields": sorted(vals)})
        return new  # type: ignore[return-value]

    def delete_pipeline(self, principal: Principal, pid: str) -> None:
        row = self.get(principal, pid, "workspace.update")
        with self.ctx.db.begin() as conn:
            conn.execute(update(DP).where(DP.c.id == row["id"]).values(deleted_at=now_iso(), etag=new_etag()))
        self.ctx.audit.record(principal, "deployment_pipeline.deleted", resource_type="deployment_pipeline", resource_id=row["id"])

    def list_pipelines(self, principal: Principal, *, limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
        with self.ctx.db.connect() as conn:
            rows, nxt = paginate(conn, select(DP).where(DP.c.deleted_at.is_(None)), DP, limit=limit, cursor=cursor)
        return [r for r in rows if any(principal.can("workspace.read", workspace_id=s["workspace_id"]) for s in r["stages"])], nxt

    def deployments(self, principal: Principal, pid: str, *, limit: int = 50) -> list[dict[str, Any]]:
        row = self.get(principal, pid)
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, select(DEP).where(DEP.c.pipeline_id == row["id"]).order_by(DEP.c.created_at.desc()).limit(limit))

    def deployment(self, principal: Principal, did: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            d = get_by_id(conn, DEP, did)
        if not d:
            raise NotFound(f"deployment {did} not found")
        self.get(principal, d["pipeline_id"])
        return d

    # ------------------------------------------------------------------ deploy
    def deploy(self, principal: Principal, pid: str, req: DeployRequest) -> dict[str, Any]:
        """Promote the source stage's workspace to the target stage (or plan it with ``dry_run``).  Returns the deployment record."""
        pipe = self.get(principal, pid)
        order = [s["stage"] for s in pipe["stages"]]
        frm = req.from_stage or order[0]
        if frm not in order:
            raise ValidationFailed(f"unknown stage '{frm}'", details={"stages": order})
        i = order.index(frm)
        to = req.to_stage or (order[i + 1] if i + 1 < len(order) else None)
        if to is None or to not in order:
            raise ValidationFailed(f"there is no stage after '{frm}'" if to is None else f"unknown stage '{to}'", details={"stages": order})
        j = order.index(to)
        if j <= i:
            raise ValidationFailed("promotion goes forward only (e.g. dev -> test -> prod)", details={"stages": order})
        if j != i + 1 and not req.allow_skip:
            raise ValidationFailed(f"'{to}' is not the stage after '{frm}'", remediation="promote stage by stage, or pass allow_skip=true")
        src_ws, dst_ws = pipe["stages"][i]["workspace_id"], pipe["stages"][j]["workspace_id"]
        principal.require("workspace.export", workspace_id=src_ws)
        principal.require("workspace.import", workspace_id=dst_ws)
        rules = normalise_rules(pipe["rules"], order)
        record = {"id": new_id("dep"), "pipeline_id": pipe["id"], "from_stage": frm, "to_stage": to, "source_workspace_id": src_ws, "target_workspace_id": dst_ws,
                  "state": "running", "dry_run": req.dry_run, "plan": {}, "result": {}, "created_by": principal.id, "created_at": now_iso(), "finished_at": None}
        with self._guard:
            lock = self._locks.setdefault(dst_ws, threading.Lock())
        if not lock.acquire(blocking=False):
            raise Conflict("another deployment into this workspace is running")
        try:
            with self.ctx.db.begin() as conn:
                insert_row(conn, DEP, record)
            try:
                plan, result = self._run(principal, pipe, rules, frm, to, src_ws, dst_ws, req)
                failed = [a for a in result["applied"] if a["action"] == "failed"]
                state = "failed" if failed or plan["problems"] and not req.dry_run else "planned" if req.dry_run else "succeeded"
            except OlcfError as exc:
                plan, result, state = {"problems": [exc.message], "details": exc.details}, {"applied": []}, "failed"
                self._finish(record["id"], state, plan, result)
                self._audit(principal, pipe, record["id"], state, frm, to)
                raise
            self._finish(record["id"], state, plan, result)
        finally:
            lock.release()
        self._audit(principal, pipe, record["id"], state, frm, to)
        out = self.deployment(principal, record["id"])
        if state == "failed":
            raise Conflict("the deployment did not complete", details={"deployment_id": record["id"], "problems": plan["problems"][:20],
                                                                      "failed": [a for a in result["applied"] if a["action"] == "failed"][:20]})
        return out

    def _finish(self, did: str, state: str, plan: dict[str, Any], result: dict[str, Any]) -> None:
        with self.ctx.db.begin() as conn:
            conn.execute(update(DEP).where(DEP.c.id == did).values(state=state, plan=plan, result=result, finished_at=now_iso()))

    def _audit(self, principal: Principal, pipe: dict[str, Any], did: str, state: str, frm: str, to: str) -> None:
        self.ctx.audit.record(principal, "deployment.completed", resource_type="deployment", resource_id=did, outcome="success" if state in ("succeeded", "planned") else "failure",
                              details={"pipeline": pipe["name"], "from": frm, "to": to, "state": state})
        self.ctx.events.publish("deployment.completed", {"pipeline": pipe["name"], "from": frm, "to": to, "state": state}, subject=did)

    # ------------------------------------------------------------------ internals
    def _run(self, principal: Principal, pipe: dict[str, Any], rules: Rules, frm: str, to: str, src_ws: str, dst_ws: str, req: DeployRequest) -> tuple[dict[str, Any], dict[str, Any]]:
        copy_connections = rules.get("connection_mode") == "copy"
        files, units = workspace_io.build_tree(self.ctx, src_ws, mode="git", notebook_format="ipynb", connections=copy_connections)
        include, exclude = set(rules.get("include_types", [])), set(rules.get("exclude_types", []))
        keep: set[str] = set()
        for key, u in units.items():
            if u["kind"] == "item" and ((include and u["type"] not in include) or u["type"] in exclude):
                continue
            if req.items and u["kind"] == "item" and not any(x in (u["item_id"], u["name"], key) for x in req.items):
                continue
            if req.items and u["kind"] == "connection":
                continue
            keep.add(key)
        if req.items:
            unknown = [x for x in req.items if not any(x in (u["item_id"], u["name"], k) for k, u in units.items() if u["kind"] == "item")]
            if unknown:
                raise NotFound(f"item(s) not found in the source workspace: {', '.join(x[:60] for x in unknown[:10])}")
        groups = {k: v for k, v in workspace_io.group_units(files).items() if k in keep}
        existing = treeapply.workspace_units(self.ctx, dst_ws)
        incoming: dict[str, dict[str, bytes] | None] = dict(groups)
        if rules.get("delete_missing"):
            if req.items:
                raise ValidationFailed("delete_missing cannot be combined with an item selection")
            for key, u in existing.items():
                if u["kind"] == "item" and key not in groups and not ((include and u["type"] not in include) or u["type"] in exclude):
                    incoming[key] = None

        problems: list[str] = []
        warnings: list[str] = []
        name_map, extra_ids = self._connection_plan(rules, to, groups, dst_ws, src_ws, copy_connections, problems)
        vstage = rules.get("variable_stage", {}).get(to, to)
        self._check_variables(groups, vstage, problems, warnings)
        plan = {"from": frm, "to": to, "variable_stage": vstage, "units": sorted(groups), "deletes": sorted(k for k, v in incoming.items() if v is None),
                "connection_map": name_map, "problems": problems, "warnings": warnings}
        if problems and not req.dry_run:
            raise ValidationFailed(f"the deployment cannot proceed: {len(problems)} problem(s)", details={"problems": problems[:50]})
        result = treeapply.apply_units(self.ctx, principal, dst_ws, incoming, existing, connection_map=name_map, extra_id_map=extra_ids, dry_run=req.dry_run,
                                       comment=f"deploy {pipe['name']}: {frm} -> {to}" + (f" ({req.comment})" if req.comment else ""))
        if not req.dry_run and not [a for a in result["applied"] if a["action"] == "failed"]:
            self._set_stage(principal, dst_ws, vstage)
        return plan, result

    def _set_stage(self, principal: Principal, ws_id: str, stage: str) -> None:
        principal.require("workspace.update", workspace_id=ws_id)
        with self.ctx.db.begin() as conn:
            row = get_by_id(conn, S.workspaces, ws_id)
            if row is None:
                raise NotFound("target workspace not found")
            settings = dict(row.get("settings") or {})
            if settings.get("stage") == stage:
                return
            settings["stage"] = stage
            conn.execute(update(S.workspaces).where(S.workspaces.c.id == ws_id).values(settings=settings, etag=new_etag(), updated_at=now_iso()))
        self.ctx.audit.record(principal, "workspace.updated", resource_type="workspace", resource_id=ws_id, workspace_id=ws_id, details={"fields": ["settings.stage"]})

    def _connection_plan(self, rules: Rules, to: str, groups: dict[str, dict[str, bytes]], dst_ws: str, src_ws: str, copy_connections: bool,
                         problems: list[str]) -> tuple[dict[str, str], dict[str, str]]:
        """Connection remapping: ``(name map, id map)`` for references found in the promoted definitions.  A reference without a counterpart is a problem."""
        conns = self.ctx.service("connections")
        mapping: dict[str, str] = dict(rules.get("connections", {}).get(to, {}))
        ids: set[str] = set()
        names: set[str] = set()
        copied = set()
        for key, files in groups.items():
            if key.startswith("connections/"):
                try:
                    copied.add(json.loads(files[""]).get("name"))
                except (ValueError, KeyError):
                    pass
                continue
            try:
                p = treeapply.parse_unit(self.ctx, key, files)
            except ValidationFailed as exc:
                problems.append(exc.message)
                continue
            defn = {"settings": p.definition.get("settings")} if p.meta["type"] == "notebook" else p.definition
            a, b = treeapply.collect_connection_refs(defn)
            ids |= a
            names |= b
        name_map: dict[str, str] = {}
        id_map: dict[str, str] = {}

        def target_exists(name: str) -> bool:
            try:
                conns.resolve(name, dst_ws)
                return True
            except NotFound:
                return False

        for cid in sorted(ids):
            try:
                src = conns.get(cid)
            except NotFound:
                problems.append(f"connection {cid} is referenced but does not exist")
                continue
            if src["workspace_id"] is None and cid not in mapping and src["name"] not in mapping:
                continue  # an organisation-wide connection is valid in every workspace
            if src["workspace_id"] not in (None, src_ws) and src["workspace_id"] != dst_ws:
                problems.append(f"connection '{src['name']}' belongs to another workspace")
                continue
            target = mapping.get(cid) or mapping.get(src["name"]) or src["name"]
            if src["workspace_id"] == dst_ws:
                continue
            if target in copied and copy_connections:
                name_map[src["name"]] = target
                continue
            try:
                id_map[cid] = conns.resolve(target, dst_ws)["id"]
                name_map[src["name"]] = target
            except NotFound:
                problems.append(f"connection '{src['name']}' has no counterpart '{target}' in the target workspace" + ("" if copy_connections else " (add a rule or create it)"))
        for name in sorted(names):
            target = mapping.get(name, name)
            if target != name:
                name_map[name] = target
            if not (target_exists(target) or (copy_connections and target in copied)):
                problems.append(f"connection '{target}' (referenced as '{name}') does not exist in the target workspace")
        name_map.update({k: v for k, v in mapping.items() if k not in name_map and not k.startswith("con_")})
        return name_map, id_map

    def _check_variables(self, groups: dict[str, dict[str, bytes]], vstage: str, problems: list[str], warnings: list[str]) -> None:
        """Every promoted variable set must be usable in the target stage: the stage exists, required variables have a value and referenced secrets exist."""
        for key, files in groups.items():
            if not key.endswith(".variable_set"):
                continue
            try:
                p = treeapply.parse_unit(self.ctx, key, files)
            except ValidationFailed:
                continue  # reported by apply_units
            d = var.normalise_definition(p.definition)
            name = p.meta["name"]
            if vstage not in d["stages"]:
                problems.append(f"variable set '{name}' has no stage '{vstage}' (stages: {', '.join(d['stages'])})")
                continue
            for v in d["variables"]:
                ov = (v.get("overrides") or {}).get(vstage)
                has = ov is not None or "default" in v
                if v.get("required") and not has:
                    problems.append(f"variable set '{name}': required variable '{v['name']}' has no value for stage '{vstage}'")
                if v["type"] == "secret" and has:
                    ref = ov if ov is not None else v["default"]
                    try:
                        self.ctx.secrets.get_meta(ref)
                    except OlcfError:
                        problems.append(f"variable set '{name}': secret of '{v['name']}' for stage '{vstage}' does not exist")
                    if ov is None and vstage != d.get("default_stage"):
                        warnings.append(f"variable set '{name}': secret variable '{v['name']}' uses its default for stage '{vstage}' (no stage override)")

