# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of environments: publish (immutable, hash-pinned lock), publications, lock, resolve, verify, policy and ``%pip`` conversion.

Permissions: reading needs ``item.read`` in the environment's workspace; ``publish`` and ``requirements/from-notebook`` (apply) need ``item.update``.
The effective package-source policy is read-only here; administrators change it with ``PUT /v1/admin/settings`` (``environments.*``) or
``settings.security`` in the static configuration.
"""

from __future__ import annotations

import re
import threading
from typing import Any

from fastapi import APIRouter, Query, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx, etag_header
from olcf.core.errors import Forbidden, NotFound
from olcf.core.models import ItemUpdate
from olcf.core.redaction import redact_obj
from olcf.notebooks import envspec
from olcf.notebooks.environments import EnvironmentService

router = APIRouter(tags=["environments"])
_SHA = re.compile(r"[0-9a-f]{64}")


def svc(ctx: Ctx) -> EnvironmentService:
    return ctx.service("environments")


class PublishRequest(BaseModel):
    """Request body for publishing an environment: a comment, a dry-run switch and ``force`` to create a new version even when the resolved lock is identical.
    """
    model_config = ConfigDict(extra="forbid")
    comment: str = Field("", max_length=500)
    dry_run: bool = False
    force: bool = Field(False, description="create a new version even when the resolved lock is identical to the latest one")
    verify: bool = Field(False, description="also install the lock into a scratch layer on the control plane to prove it is installable")
    wait: bool = Field(True, description="false returns 202 + operation_id and resolves in the background")


class FromNotebook(BaseModel):
    """Request body for deriving an environment from a notebook's imports; ``apply`` writes the result into the environment."""
    model_config = ConfigDict(extra="forbid")
    notebook_id: str
    apply: bool = False


@router.get("/environments/policy")
def policy(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The effective package-source policy (allowed indexes, public Internet, approved wheelhouses)."""
    vis = principal.visible_workspaces("item.read")
    if vis is not None and not vis:
        raise Forbidden("permission 'item.read' is required")
    pol = svc(ctx).policy()
    return {**pol.to_dict(), "pip_policy_default": svc(ctx)._kv("environments.pip_policy_default", "declared"), "pip_policy_max": svc(ctx)._kv("environments.pip_policy_max", "allow")}  # noqa: SLF001


@router.get("/environments/jars/{sha256}")
def download_jar(sha256: str, principal: Authed, ctx: Ctx) -> FileResponse:
    """Content-addressed jar pinned by an environment (``jars.files``): nodes download it by hash and verify it again.  Worker agents and administrators only."""
    if principal.kind != "worker":
        principal.require("runtime.manage")
    if not _SHA.fullmatch(sha256):
        raise NotFound("jar not found")
    path = svc(ctx).env_jar_path(sha256)
    if path is None:
        raise NotFound("jar not found", remediation="publish the environment again")
    return FileResponse(path, media_type="application/java-archive", filename=f"{sha256}.jar", headers={"X-Content-SHA256": sha256})


@router.post("/environments/{item_id}/publish")
def publish(item_id: str, body: PublishRequest, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Resolve and pin the environment.  ``201`` with the new publication (``200`` when nothing changed or for ``dry_run``), ``202`` for ``wait=false``."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.update")
    if not body.wait:
        ops = ctx.service("operations")
        oid = ops.create("environment.publish", principal.id, f"publishing {row['name']}")

        def work() -> None:
            try:
                res = s.publish(principal, row, comment=body.comment, dry_run=body.dry_run, force=body.force, verify=body.verify)
                ops.update(oid, state="succeeded", progress=100, result=redact_obj(res), message="published")
            except Exception as exc:  # noqa: BLE001 - recorded on the operation
                ops.update(oid, state="failed", progress=100, error={"code": getattr(exc, "code", "internal"), "message": redact_obj(getattr(exc, "message", str(exc))),
                                                                      "remediation": getattr(exc, "remediation", None)})

        threading.Thread(target=work, name=f"env-publish-{oid}", daemon=True).start()
        response.status_code = 202
        return {"operation_id": oid}
    res = s.publish(principal, row, comment=body.comment, dry_run=body.dry_run, force=body.force, verify=body.verify)
    response.status_code = 201 if res["created"] else 200
    return res


@router.get("/environments/{item_id}/publications")
def publications(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = svc(ctx).item(principal, item_id, "item.read")
    return {"items": svc(ctx).publications(row), "next_cursor": None}


@router.get("/environments/{item_id}/publications/{version}")
def publication(item_id: str, version: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    pub = s.publication(row, version)
    return {"publication": pub, "lock": s.public_lock(s.get_lock(pub["lock_hash"]))}


@router.get("/environments/{item_id}/lock")
def latest_lock(item_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """The latest published lock (immutable; its ``lock_hash`` is the ETag)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    pubs = s.publications(row)
    if not pubs:
        from olcf.core.errors import NotFound

        raise NotFound("this environment has not been published", remediation=f"POST /v1/environments/{row['id']}/publish")
    lk = s.get_lock(pubs[0]["lock_hash"])
    response.headers["ETag"] = etag_header(lk["lock_hash"].replace("sha256:", "")[:24])
    return s.public_lock(lk)


@router.get("/environments/{item_id}/resolve")
def resolve(item_id: str, principal: Authed, ctx: Ctx, version: int | None = None) -> dict[str, Any]:
    """The ``EnvironmentSpec`` jobs receive (runtime request, spark conf, variables, lock and layer instructions)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    return redact_obj(s.resolve(row["id"], version=version, workspace_id=row["workspace_id"]))


@router.get("/environments/{item_id}/diff")
def diff(item_id: str, a: int, b: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    return s.diff(row, a, b)


@router.post("/environments/{item_id}/verify")
def verify(item_id: str, principal: Authed, ctx: Ctx, version: int | None = Query(None)) -> dict[str, Any]:
    """Install a published lock into a scratch layer on the control plane (``pip install --require-hashes --no-deps``) to prove it installs."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.update")
    pubs = s.publications(row)
    pub = s.publication(row, version) if version else (pubs[0] if pubs else None)
    if pub is None:
        from olcf.core.errors import NotFound

        raise NotFound("this environment has not been published")
    return s.verify_lock(s.get_lock(pub["lock_hash"]))


@router.post("/environments/{item_id}/requirements/from-notebook")
def from_notebook(item_id: str, body: FromNotebook, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Convert the ``%pip install`` lines of a notebook into environment requirements (``apply`` adds them to the definition)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.update" if body.apply else "item.read")
    nbs = ctx.service("notebooks")
    nb_row = nbs.item(principal, body.notebook_id, "item.read")
    if nb_row["workspace_id"] != row["workspace_id"]:
        from olcf.core.errors import NotFound

        raise NotFound(f"notebook {body.notebook_id} not found in this workspace")
    found = s.requirements_from_notebook(nbs.load(nb_row).nb)
    out: dict[str, Any] = {**found, "applied": False}
    if body.apply and found["requirements"]:
        d = envspec.normalise_definition(row["definition"])
        py = dict(d.get("python") or {})
        py["requirements"] = envspec.normalise_requirements([*(py.get("requirements") or []), *found["requirements"]])
        ctx.service("items").update(principal, row["id"], ItemUpdate(definition={**d, "python": py}, comment=f"requirements from notebook {nb_row['name']}"), None)
        out["applied"] = True
        out["requirements_now"] = py["requirements"]
    return out
