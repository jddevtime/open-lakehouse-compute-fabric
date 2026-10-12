# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Variable sets (``variable_set`` items): typed variables with per-stage overrides, secrets by reference only, and ``VariableService``.

Definition::

    {"stages": ["dev", "test", "prod"], "default_stage": "dev",
     "variables": [{"name": "storage_container", "type": "string", "default": "raw-dev", "overrides": {"prod": "raw"}, "description": "...", "required": false},
                   {"name": "api_key", "type": "secret", "default": "sec_01J...", "overrides": {"prod": "sec_01K..."}}]}

* Types: ``string | integer | number | boolean | array | object | secret``.  A ``secret`` variable holds a **secret id** (a reference into the secret store),
  never a value; the definition validator rejects anything else, and non-secret variables whose name or value looks like a credential.
* ``VariableService.resolve(ref, stage=None, *, workspace_id=None, principal=None)`` (registered as ``ctx.service("variables")`` for ORCH and the runners) returns
  ``{id, name, stage, values: {name: typed value}, secrets: {name: secret_id}, variables: [...]}``.  Secret *values* are never returned: callers pass ``secrets`` on as
  ``JobSubmission.secret_refs`` so the controller resolves them just in time.
* The stage defaults to the workspace's active stage (``workspace.settings.stage``, set by ``PUT /v1/workspaces/{ws}/stage`` or by a deployment) and then to
  ``default_stage``.
* :func:`substitute` expands ``${var:NAME}`` placeholders in strings / documents (a string that is exactly one placeholder keeps the variable's type).
"""

from __future__ import annotations

import json
import re
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx
from olcf.core.context import AppContext
from olcf.core.errors import NotFound, ValidationFailed
from olcf.notebooks import params as nbparams
from olcf.security.rbac import Principal

TYPES = ("string", "integer", "number", "boolean", "array", "object", "secret")
DEFAULT_STAGES = ["dev", "test", "prod"]
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_STAGE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
_SECRET_ID = re.compile(r"^sec_[A-Za-z0-9]{10,40}$")
_SECRETISH_NAME = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential|connection[_-]?string)")
_PLACEHOLDER = re.compile(r"\$\{var:([A-Za-z_][A-Za-z0-9_]{0,63})\}")


def _check_value(vtype: str, value: Any, where: str) -> Any:
    if vtype == "secret":
        if not isinstance(value, str) or not _SECRET_ID.match(value):
            raise ValidationFailed(f"{where}: a secret variable holds a secret id (sec_...), never a value",
                                   remediation="create the secret with POST /v1/secrets and put its id here")
        return value
    nbparams.check_json_value(value, where)
    ok = {
        "string": isinstance(value, str), "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool), "boolean": isinstance(value, bool),
        "array": isinstance(value, list), "object": isinstance(value, dict),
    }[vtype]
    if not ok:
        raise ValidationFailed(f"{where}: expected {vtype}, got {type(value).__name__}")
    if isinstance(value, str):
        from olcf.notebooks.secretscan import scan_text

        hits = scan_text(value)
        if hits:
            raise ValidationFailed(f"{where}: the value looks like a credential ({hits[0].rule}); store it as a secret and use a secret variable",
                                   remediation="variables are stored in clear text and exported to Git")
    return value


def normalise_definition(defn: Any) -> dict[str, Any]:
    """``validate`` hook of the ``variable_set`` type (idempotent)."""
    if defn is None:
        defn = {}
    if not isinstance(defn, dict):
        raise ValidationFailed("a variable set definition must be an object")
    bad = [k for k in defn if k not in ("stages", "default_stage", "variables", "description") and not str(k).startswith("x-")]
    if bad:
        raise ValidationFailed(f"unknown variable set keys {sorted(bad)}")
    stages = defn.get("stages", DEFAULT_STAGES)
    if not isinstance(stages, list) or not stages or len(stages) > 12 or not all(isinstance(s, str) and _STAGE.match(s) for s in stages) or len(set(stages)) != len(stages):
        raise ValidationFailed("stages must be a list of 1-12 unique names (letters, digits, '-', '_')")
    out: dict[str, Any] = {"stages": list(stages)}
    ds = defn.get("default_stage")
    if ds is not None:
        if ds not in stages:
            raise ValidationFailed(f"default_stage '{ds}' is not one of the stages {stages}")
        out["default_stage"] = ds
    variables = defn.get("variables", [])
    if not isinstance(variables, list) or len(variables) > 1000:
        raise ValidationFailed("variables must be a list of at most 1000 entries")
    seen: set[str] = set()
    norm: list[dict[str, Any]] = []
    for i, v in enumerate(variables):
        if not isinstance(v, dict):
            raise ValidationFailed(f"variables[{i}] must be an object")
        extra = set(v) - {"name", "type", "default", "overrides", "description", "required"}
        if extra:
            raise ValidationFailed(f"variables[{i}] has unknown keys {sorted(extra)}")
        name = v.get("name")
        if not isinstance(name, str) or not _NAME.match(name):
            raise ValidationFailed(f"variables[{i}]: invalid name {str(name)[:40]!r}", remediation="letters, digits and underscores; must not start with a digit")
        if name in seen:
            raise ValidationFailed(f"variable '{name}' is defined twice")
        seen.add(name)
        vtype = v.get("type", "string")
        if vtype not in TYPES:
            raise ValidationFailed(f"variable '{name}': type must be one of {TYPES}")
        if vtype != "secret" and _SECRETISH_NAME.search(name) and (v.get("default") not in (None, "") or v.get("overrides")):
            raise ValidationFailed(f"variable '{name}' looks like a credential; make it type 'secret' (a reference to the secret store)")
        entry: dict[str, Any] = {"name": name, "type": vtype}
        if "default" in v and v["default"] is not None:
            entry["default"] = _check_value(vtype, v["default"], f"variable '{name}' default")
        ov = v.get("overrides")
        if ov:
            if not isinstance(ov, dict):
                raise ValidationFailed(f"variable '{name}': overrides must be an object stage -> value")
            ovn = {}
            for st, val in ov.items():
                if st not in stages:
                    raise ValidationFailed(f"variable '{name}': override for unknown stage '{st}'", details={"stages": stages})
                ovn[st] = _check_value(vtype, val, f"variable '{name}' override[{st}]")
            entry["overrides"] = ovn
        if v.get("required"):
            entry["required"] = True
        if isinstance(v.get("description"), str) and v["description"]:
            entry["description"] = v["description"][:500]
        norm.append(entry)
    out["variables"] = norm
    if isinstance(defn.get("description"), str):
        out["description"] = defn["description"]
    for k, val in defn.items():
        if str(k).startswith("x-"):
            out[k] = val
    return out


def substitute(obj: Any, values: dict[str, Any], *, secrets: dict[str, str] | None = None) -> Any:
    """Expand ``${var:NAME}`` in ``obj`` (strings, lists, dicts).  Referencing a secret variable is an error: pass secrets as ``secret_refs`` instead."""
    secrets = secrets or {}
    if isinstance(obj, str):
        m = _PLACEHOLDER.fullmatch(obj)
        if m:
            return _lookup(m.group(1), values, secrets)

        def rep(mm: re.Match[str]) -> str:
            v = _lookup(mm.group(1), values, secrets)
            return v if isinstance(v, str) else json.dumps(v)

        return _PLACEHOLDER.sub(rep, obj)
    if isinstance(obj, list):
        return [substitute(x, values, secrets=secrets) for x in obj]
    if isinstance(obj, dict):
        return {k: substitute(v, values, secrets=secrets) for k, v in obj.items()}
    return obj


def _lookup(name: str, values: dict[str, Any], secrets: dict[str, str]) -> Any:
    if name in secrets:
        raise ValidationFailed(f"variable '{name}' is a secret and cannot be substituted into text", remediation="pass it through secret_refs")
    if name not in values:
        raise ValidationFailed(f"unknown variable '{name}'", details={"known": sorted(values)})
    return values[name]


class VariableService:
    """``ctx.service("variables")``."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def item(self, principal: Principal | None, item_id: str, perm: str = "item.read") -> dict[str, Any]:
        row = self.ctx.service("items").get(item_id)
        if row["type"] != "variable_set":
            raise NotFound(f"variable set {item_id} not found")
        if principal is not None:
            principal.require(perm, workspace_id=row["workspace_id"], item_id=row["id"])
        return row

    def workspace_stage(self, workspace_id: str) -> str | None:
        ws = self.ctx.service("workspaces").resolve(workspace_id)
        st = (ws.get("settings") or {}).get("stage")
        return st if isinstance(st, str) and _STAGE.match(st) else None

    def resolve(self, ref: str, stage: str | None = None, *, workspace_id: str | None = None, principal: Principal | None = None, check_secrets: bool = True) -> dict[str, Any]:
        """Resolve a variable set (item id, or name within ``workspace_id``) for ``stage``.  See the module docstring for the result shape."""
        items = self.ctx.service("items")
        if ref.startswith("itm_"):
            row = items.get(ref)
            if workspace_id and row["workspace_id"] != workspace_id:
                raise NotFound(f"variable set {ref} not found in this workspace")
        else:
            if not workspace_id:
                raise NotFound(f"variable set '{ref}' needs a workspace to be resolved by name")
            row = items.resolve(workspace_id, ref, "variable_set")
        if row["type"] != "variable_set":
            raise NotFound(f"item {row['id']} is not a variable set")
        if principal is not None:
            principal.require("item.read", workspace_id=row["workspace_id"], item_id=row["id"])
        d = normalise_definition(row["definition"])
        stages = d["stages"]
        chosen = stage or self.workspace_stage(row["workspace_id"]) or d.get("default_stage")
        if chosen is not None and chosen not in stages:
            if stage:
                raise ValidationFailed(f"unknown stage '{stage}'", details={"stages": stages})
            chosen = d.get("default_stage")  # the workspace stage is not one of this set's stages: fall back to its default
        values: dict[str, Any] = {}
        secrets: dict[str, str] = {}
        detail: list[dict[str, Any]] = []
        missing: list[str] = []
        for v in d["variables"]:
            source = None
            val = None
            if chosen and chosen in (v.get("overrides") or {}):
                val, source = v["overrides"][chosen], "override"
            elif "default" in v:
                val, source = v["default"], "default"
            if source is None:
                if v.get("required"):
                    missing.append(v["name"])
                detail.append({"name": v["name"], "type": v["type"], "source": None, "description": v.get("description", "")})
                continue
            if v["type"] == "secret":
                if check_secrets:
                    try:
                        self.ctx.secrets.get_meta(val)
                    except Exception as exc:  # noqa: BLE001
                        raise ValidationFailed(f"secret of variable '{v['name']}' does not exist", remediation="create the secret or fix the reference") from exc
                secrets[v["name"]] = val
                detail.append({"name": v["name"], "type": "secret", "source": source, "secret_ref": val, "description": v.get("description", "")})
            else:
                values[v["name"]] = val
                detail.append({"name": v["name"], "type": v["type"], "source": source, "value": val, "description": v.get("description", "")})
        if missing:
            raise ValidationFailed(f"required variable(s) without a value for stage '{chosen}': {', '.join(missing)}", details={"missing": missing, "stage": chosen},
                                   remediation="set a default or an override for that stage")
        return {"id": row["id"], "name": row["name"], "workspace_id": row["workspace_id"], "version": row["version"], "stage": chosen, "stages": stages,
                "values": values, "secrets": secrets, "variables": detail}

    def substitute(self, obj: Any, ref: str, stage: str | None = None, *, workspace_id: str | None = None, principal: Principal | None = None) -> Any:
        res = self.resolve(ref, stage, workspace_id=workspace_id, principal=principal)
        return substitute(obj, res["values"], secrets=res["secrets"])


class StageBody(BaseModel):
    """Request body for setting the active stage of a variable set (or clearing it)."""
    model_config = ConfigDict(extra="forbid")
    stage: str | None = Field(..., pattern=r"^[A-Za-z0-9_-]{1,32}$")


router = APIRouter(tags=["variable-sets"])


@router.get("/variable-sets/{item_id}/resolve")
def resolve(item_id: str, principal: Authed, ctx: Ctx, stage: str | None = Query(None, pattern=r"^[A-Za-z0-9_-]{1,32}$")) -> dict[str, Any]:
    """Typed values for ``stage`` (default: the workspace's active stage, then the set's default).  Secrets are returned as references only."""
    s: VariableService = ctx.service("variables")
    row = s.item(principal, item_id, "item.read")
    return s.resolve(row["id"], stage, workspace_id=row["workspace_id"], principal=principal, check_secrets=False)


@router.get("/workspaces/{ws}/stage")
def get_stage(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.read", workspace_id=row["id"])
    return {"workspace_id": row["id"], "stage": (row.get("settings") or {}).get("stage")}


@router.put("/workspaces/{ws}/stage")
def set_stage(ws: str, body: StageBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Select the stage (dev / test / prod ...) whose variable overrides apply to runs in this workspace."""
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.update", workspace_id=row["id"])
    settings = dict(row.get("settings") or {})
    if body.stage is None:
        settings.pop("stage", None)
    else:
        settings["stage"] = body.stage
    ctx.service("workspaces").update(principal, row["id"], {"settings": settings}, None)
    return {"workspace_id": row["id"], "stage": body.stage}
