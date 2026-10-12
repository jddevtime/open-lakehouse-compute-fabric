# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Interactive notebook sessions (CONTRACTS 4.4) and the pooled hidden SQL sessions behind ``ComputeService.run_spark_sql``.

Model
-----
A session is one Jupyter-style kernel on a worker node (``start_kernel`` / ``kernel_execute`` ... ``kernel_shutdown``, CONTRACTS 4.2). Placement reuses the pure
planner (:func:`olcf.compute.planner.plan`) so a session is admitted exactly like a job (pools, labels, reserved pools, resource headroom, runtime-ready nodes) but
*immediately*: an interactive request that cannot be placed fails fast with the planner's reason instead of queueing.

Everything an HTTP replica needs lives in the database so any replica can serve any client:

* ``sessions`` holds state, placement, resources and the Spark application;
* ``session_events`` is the bounded output relay. The replica that owns the worker channel appends each ``kernel_event`` (one row per output), every replica that owns a
  browser WebSocket tails the table. A reconnecting browser receives the current status plus the replay of the newest ``compute.session_replay_events`` (500) outputs.
* ``msg_id`` of a ``kernel_execute`` is ``<cell_id>~<random>``; the cell is recovered from the agent's ``parent_msg_id`` so no per-replica map is needed.

Lifecycle: ``starting`` -> ``idle`` <-> ``busy`` -> ``dead``. ``dead`` is terminal and records the reason. A session dies when the user deletes it, it idles beyond
``compute.session_idle_timeout_seconds`` (hidden SQL sessions: ``compute.sql_session_idle_timeout_seconds``), the agent reports the kernel dead, the node restarts
(``hello`` without the kernel), or the node stays lost beyond ``compute.worker_lost_grace_seconds``. Dying releases the resources (planner allocation), revokes the
session's job token, finishes its Spark application row and publishes ``session.stopped``.

``run_spark_sql`` executes SQL in a hidden session pooled per ``(workspace, lakehouse, runtime)``; results are bounded (``limit`` rows) and returned as JSON-safe values.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update

from olcf.compute import lineage_events
from olcf.compute import protocol as P
from olcf.compute import schema as C
from olcf.compute.errors import ResourceUnsatisfiable, WorkerOffline, WorkerTimeout
from olcf.compute.planner import Blocked, JobView, plan
from olcf.compute.providers.base import PrepareRequest
from olcf.compute.registry import ACTIVE_SESSION_STATES, ComputeRegistry, NodeInfo
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, OlcfError, Unavailable
from olcf.core.ids import new_id
from olcf.core.models import JobKind, JobSubmission, LakehouseRef, ResourceRequest, RuntimeRequest
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import age_seconds, now_iso
from olcf.db.util import fetch_all, fetch_one, insert_row, paginate
from olcf.security.rbac import (
    LEGACY_JOB_PERMISSIONS,
    SQL_KERNEL_PERMISSIONS,
    SYSTEM_PRINCIPAL,
    Principal,
    fixed_ceiling,
)

log = logging.getLogger("olcf.compute.sessions")

MSG_SEP = "~"
INTERACTIVE_QUEUE = "__interactive__"
_CELL_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_OUTPUT_KINDS = ("output", "reply", "clear")
SQL_MARK = "@@OLCF_SQL_RESULT@@"
MAX_SQL_ROWS = 100_000

#: Executed inside the kernel for ``run_spark_sql``: runs the statement, serialises at most ``limit + 1`` rows with Spark's own JSON writer (dates/decimals are
#: handled by Spark), and prints one marker line the controller parses. The SQL travels as a JSON string literal (valid Python), never interpolated into code.
_SQL_TEMPLATE = '''
import json as _olcf_json
_olcf_df = spark.sql(_olcf_json.loads({sql_literal}))
_olcf_cols = [{{"name": f.name, "type": f.dataType.simpleString()}} for f in _olcf_df.schema.fields]
_olcf_rows = []
if _olcf_cols:
    for _olcf_line in _olcf_df.limit({limit_plus}).toJSON().collect():
        _olcf_obj = _olcf_json.loads(_olcf_line)
        _olcf_rows.append([_olcf_obj.get(c["name"]) for c in _olcf_cols])
print("{mark}" + _olcf_json.dumps({{"columns": _olcf_cols, "rows": _olcf_rows}}, default=str))
del _olcf_df, _olcf_cols, _olcf_rows
'''


class SessionCreate(BaseModel):
    """Request body that starts an interactive notebook session: workspace, notebook, engine, runtime, resources, default lakehouse and placement.
    """
    workspace_id: str
    notebook_id: str | None = None
    engine: Literal["spark", "python"] = "spark"
    runtime: RuntimeRequest = Field(default_factory=RuntimeRequest)
    resources: ResourceRequest = Field(default_factory=ResourceRequest)
    default_lakehouse: LakehouseRef | None = None
    #: Placement constraints (``pool``, ``labels``, ``node``) and an optional ``provider`` (local | standalone | ...).
    placement: dict[str, Any] = Field(default_factory=dict)
    name: str | None = Field(None, max_length=255)
    idle_timeout_seconds: int | None = Field(None, ge=60)
    #: Start the SparkSession when the kernel starts (default) instead of on first use.
    eager_spark: bool = True
    #: Environment item (``itm_...[@version]``) whose dependency layer, Spark conf and variables the kernel gets. Wins over ``runtime.environment_id`` and the notebook's
    #: ``settings.environment_id``.
    environment_id: str | None = None
    #: Allow ``%pip install`` of packages outside the declared environment (``None`` = the notebook's ``settings.policy.allow_pip_install``). Refused when the
    #: administrator capped ``environments.pip_policy_max`` below ``allow``.
    allow_pip_install: bool | None = None


def make_msg_id(cell_id: str) -> str:
    if not _CELL_RE.match(cell_id):
        raise BadRequest("cell_id must be 1-64 characters of letters, digits, '.', '_' or '-'")
    return f"{cell_id}{MSG_SEP}{uuid.uuid4().hex[:12]}"


def cell_of(msg_id: str | None) -> str | None:
    if msg_id and MSG_SEP in msg_id:
        return msg_id.rsplit(MSG_SEP, 1)[0][:64]
    return None


def to_output(msg_type: str, content: dict[str, Any]) -> dict[str, Any] | None:
    """Jupyter iopub content -> nbformat v4 output dict (``None`` for messages the browser does not render)."""
    if msg_type == "stream":
        return {"output_type": "stream", "name": content.get("name") or "stdout", "text": content.get("text") or ""}
    if msg_type in ("display_data", "update_display_data"):
        out: dict[str, Any] = {"output_type": "display_data", "data": content.get("data") or {}, "metadata": content.get("metadata") or {}}
        tr = content.get("transient")
        if tr:
            out["transient"] = tr
        if msg_type == "update_display_data":
            out["update"] = True
        return out
    if msg_type == "execute_result":
        return {"output_type": "execute_result", "data": content.get("data") or {}, "metadata": content.get("metadata") or {}, "execution_count": content.get("execution_count")}
    if msg_type == "error":
        return {"output_type": "error", "ename": content.get("ename") or "Error", "evalue": content.get("evalue") or "", "traceback": content.get("traceback") or []}
    return None


class SessionService:
    """Registered as ``sessions`` and as ``compute.sessions``."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: Any, jobs: Any, providers: Any, runtimes: Any) -> None:
        self.ctx, self.registry, self.hub, self.jobs, self.providers, self.runtimes = ctx, registry, hub, jobs, providers, runtimes
        self.db = ctx.db
        self.scheduler: Any = None  # set by the module (planner inputs, wake)
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="olcf-session")
        self._cond = threading.Condition()
        self._epoch = 0  # bumped on every locally observed event so local waiters wake immediately
        self._starting: set[str] = set()
        self._since_prune: dict[str, int] = {}
        self._sql_locks: dict[str, threading.Lock] = {}
        self._sql_guard = threading.Lock()
        self._waiters: dict[str, set[tuple[asyncio.AbstractEventLoop, asyncio.Event]]] = {}
        hub.on("kernel_event", self._on_kernel_event)
        hub.on_hello(self._on_hello)

    # ================================================================== helpers
    def _s(self, key: str, default: Any) -> Any:
        return self.registry.setting(key, default)

    def get_row(self, session_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.sessions).where(C.sessions.c.id == session_id))
        if not row:
            raise NotFound(f"session {session_id} not found")
        return row

    def _row_or_none(self, session_id: str) -> dict[str, Any] | None:
        try:
            return self.get_row(session_id)
        except NotFound:
            return None

    def authorize(self, principal: Principal, session_id: str) -> dict[str, Any]:
        """Owner (with ``session.use`` in the workspace) or a platform administrator. Hidden pool sessions are administrator-only."""
        row = self.get_row(session_id)
        if principal.is_admin():
            return row
        principal.require("session.use", workspace_id=row["workspace_id"])
        if row["hidden"] or row["owner_id"] != principal.id:
            raise NotFound(f"session {session_id} not found")  # never reveal other users' sessions
        return row

    def public(self, row: dict[str, Any]) -> dict[str, Any]:
        spark = row.get("spark") or None
        out_spark = None
        if spark:
            with self.db.connect() as conn:
                app = fetch_one(conn, select(C.spark_apps.c.id).where(C.spark_apps.c.session_id == row["id"]).order_by(C.spark_apps.c.created_at.desc()).limit(1))
            out_spark = {"application_id": spark.get("application_id"), "app_name": spark.get("app_name"), "ui": f"/v1/spark-applications/{app['id']}/ui/" if app else None}
        res = row.get("resources") or {}
        lake = row.get("default_lakehouse") or None
        return {"id": row["id"], "workspace_id": row["workspace_id"], "kind": row["kind"], "hidden": bool(row["hidden"]), "owner_id": row["owner_id"], "notebook_id": row["notebook_id"],
                "name": (row.get("spec") or {}).get("name"), "engine": row["engine"], "state": row["state"], "state_reason": row["state_reason"], "runtime_id": row["runtime_id"],
                "provider": row["provider"], "cluster_id": row["cluster_id"], "worker_id": row["worker_id"],
                "resources": {k: res.get(k) for k in ("driver_cores", "driver_memory_mb", "executor_cores_total", "executor_cores", "executor_memory_mb", "executors")},
                "default_lakehouse": {"id": lake.get("id"), "name": lake.get("name")} if isinstance(lake, dict) else None, "spark": out_spark, "execution_count": row["execution_count"],
                "idle_timeout_seconds": (row.get("spec") or {}).get("idle_timeout_seconds"), "created_at": row["created_at"], "updated_at": row["updated_at"],
                "last_activity_at": row["last_activity_at"], "stopped_at": row["stopped_at"], "ws_path": f"/v1/sessions/{row['id']}/ws"}

    def list(self, principal: Principal, *, workspace_id: str | None = None, state: str | None = None, mine: bool = True, include_hidden: bool = False,
             limit: int = 50, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        t = C.sessions.c
        stmt = select(C.sessions)
        admin = principal.is_admin()
        if workspace_id:
            principal.require("session.use", workspace_id=workspace_id)
            stmt = stmt.where(t.workspace_id == workspace_id)
        elif not admin:
            vis = principal.visible_workspaces("session.use")
            if vis is not None:
                if not vis:
                    return [], None
                stmt = stmt.where(t.workspace_id.in_(sorted(vis)))
        if not admin or mine:
            stmt = stmt.where(t.owner_id == principal.id)
        if not (admin and include_hidden):
            stmt = stmt.where(t.hidden.is_(False))
        if state:
            stmt = stmt.where(t.state == state)
        with self.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, C.sessions, limit=limit, cursor=cursor, descending=True)
        return rows, nxt

    def _update(self, session_id: str, *, only_not_dead: bool = True, **vals: Any) -> bool:
        vals.setdefault("updated_at", now_iso())
        cond = [C.sessions.c.id == session_id]
        if only_not_dead:
            cond.append(C.sessions.c.state != "dead")
        with self.db.begin() as conn:
            return bool(conn.execute(update(C.sessions).where(*cond).values(**vals)).rowcount)

    def _publish(self, event: str, row: dict[str, Any], **extra: Any) -> None:
        self.ctx.events.publish(event, {"session_id": row["id"], "workspace_id": row["workspace_id"], "state": row["state"], "kind": row["kind"], "worker_id": row["worker_id"], **extra},
                                workspace_id=row["workspace_id"], subject=row["id"])

    # ================================================================== create
    def create(self, principal: Principal, body: SessionCreate, *, hidden: bool = False, pool_key: str | None = None, kind: str = "notebook",
               owner_id: str | None = None, authorized: bool = False) -> dict[str, Any]:
        ws = self.jobs._workspace(body.workspace_id)  # noqa: SLF001
        if not authorized:
            principal.require("session.use", workspace_id=ws["id"])
        item = self._load_notebook(principal, body, ws) if body.notebook_id else None
        settings = (item or {}).get("_settings") or {}
        fields = body.model_fields_set
        engine = body.engine if "engine" in fields else (settings.get("engine") or body.engine)
        resources = body.resources if "resources" in fields else ResourceRequest.model_validate({**body.resources.model_dump(), **(settings.get("resources") or {})})
        runtime_req = body.runtime if "runtime" in fields else RuntimeRequest.model_validate({**body.runtime.model_dump(), **(settings.get("runtime") or {}),
                                                                                              **({"environment_id": settings["environment_id"]} if settings.get("environment_id") else {})})
        if body.environment_id:
            runtime_req = runtime_req.model_copy(update={"environment_id": body.environment_id})
        resources_defaulted = "resources" not in fields and not settings.get("resources")
        allow_pip = body.allow_pip_install if body.allow_pip_install is not None else bool((settings.get("policy") or {}).get("allow_pip_install"))
        session_policy = {"placement": dict(body.placement or {}), "allow_pip_install": bool(allow_pip)}
        self.jobs.check_pip_install_allowed(session_policy)
        lake_ref = body.default_lakehouse or (LakehouseRef.model_validate(settings["default_lakehouse"]) if isinstance(settings.get("default_lakehouse"), dict) else None)
        if not hidden:
            n = self._active_count(principal.id)
            limit = int(self._s("compute.max_sessions_per_user", 5))
            if n >= limit:
                raise Conflict(f"you already have {n} active sessions (limit {limit})", remediation="close an idle session (DELETE /v1/sessions/{id})")
        placement = dict(body.placement or {})
        provider_name = placement.get("provider") or str(self._s("compute.session_provider", "local") or "local")
        try:
            provider = self.providers.get(provider_name) if provider_name != "auto" else None
        except NotFound:
            raise BadRequest(f"provider '{provider_name}' does not exist or is disabled") from None
        if provider is not None and provider.execution != "agent":
            raise BadRequest(f"provider '{provider_name}' cannot host interactive kernels (it runs applications remotely)", remediation="use provider 'local' or 'standalone'")
        if placement.get("pool"):
            prow = self.registry.get_pool(placement["pool"])
            if prow["reserved"] and prow["workspace_ids"] and ws["id"] not in prow["workspace_ids"]:
                raise Forbidden(f"pool '{placement['pool']}' is reserved for other workspaces")
        runtime = self.runtimes.resolve(runtime_req, workspace_id=ws["id"], engine=engine)
        sub = JobSubmission(kind=JobKind.NOTEBOOK, workspace_id=ws["id"], default_lakehouse=lake_ref)
        lakes = self.jobs._resolve_lakehouses(principal, sub, ws, lake_ref)  # noqa: SLF001
        session_id = new_id("ses")
        now_t = time.time()
        with self.registry.db_lock("session-place", ttl=30.0, wait=30.0):
            placed = self._place(session_id, ws["id"], engine, resources, runtime.runtime_id, placement, provider_name, now_t, flexible=resources_defaulted and engine == "spark")
            granted = dict(placed.granted())
            granted["provider"] = placed.provider
            granted["cores"], granted["memory_mb"] = placed.driver_cores, placed.driver_memory_mb
            if placed.cluster_key:
                granted["cluster_runtime"], granted["cluster_pool"] = placed.cluster_key
            idle = int(body.idle_timeout_seconds or (self._s("compute.sql_session_idle_timeout_seconds", 900) if hidden else self._s("compute.session_idle_timeout_seconds", 3600)))
            env_vars = {str(k): str(v) for k, v in ((runtime.environment or {}).get("variables") or {}).items()}
            for k in env_vars:
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", k) or k.startswith("OLCF_"):
                    raise BadRequest(f"invalid or reserved environment variable name '{k}' in the environment")
            spec_doc = {"name": body.name or (item or {}).get("name") or ("sql" if hidden else "session"), "queue": INTERACTIVE_QUEUE, "env": env_vars, "secret_refs": {}, "tags": {},
                        "policy": session_policy, "resources_defaulted": resources_defaulted, "runtime_request": runtime_req.model_dump(exclude={"spark_conf"}), "spark_conf": dict(runtime.spark_conf),
                        "rejected_spark_conf": list(runtime.rejected_conf), "environment": runtime.environment, "dependency_lock": runtime.dependency_lock,
                        "default_lakehouse": lakes["default"], "lakehouses": lakes["all"], "lakehouse_ids": lakes["ids"], "source": {}, "idle_timeout_seconds": idle,
                        "eager_spark": body.eager_spark, "resources_request": resources.model_dump()}
            # F01: what the kernel's callback credential may ever do. A pooled SQL kernel serves every user of the workspace, so it gets a fixed read-only ceiling instead of the
            # permissions of whoever happened to start the pool; any other kernel is bounded by the credential that opened the session.
            spec_doc["credential"] = fixed_ceiling(SQL_KERNEL_PERMISSIONS, ws["id"]) if hidden else principal.credential_ceiling(ws["id"])
            ts = now_iso()
            row = {"id": session_id, "workspace_id": ws["id"], "kind": kind, "hidden": hidden, "pool_key": pool_key, "owner_id": owner_id or principal.id, "notebook_id": body.notebook_id,
                   "engine": engine, "state": "starting", "state_reason": ("starting the kernel" + (f" ({placed.note})" if getattr(placed, "note", None) else ""))[:500], "runtime_id": runtime.runtime_id, "provider": placed.provider, "cluster_id": None,
                   "worker_id": placed.node_id, "resources": granted, "default_lakehouse": lakes["default"], "spec": spec_doc, "spark": None, "token_id": None, "execution_count": 0,
                   "created_at": ts, "updated_at": ts, "last_activity_at": ts, "stopped_at": None}
            with self.db.begin() as conn:
                insert_row(conn, C.sessions, row)
        self.ctx.audit.record(principal, "session.created", resource_type="session", resource_id=session_id, workspace_id=ws["id"],
                              details={"engine": engine, "runtime": runtime.runtime_id, "provider": placed.provider, "worker_id": placed.node_id, "hidden": hidden})
        self._publish("session.created", row)
        with self._cond:
            self._starting.add(session_id)
        self._pool.submit(self._start_kernel, session_id)
        return self.get_row(session_id)

    def _active_count(self, owner_id: str) -> int:
        with self.db.connect() as conn:
            return int(conn.execute(select(func.count()).select_from(C.sessions).where(C.sessions.c.owner_id == owner_id, C.sessions.c.hidden.is_(False),
                                                                                    C.sessions.c.state.in_(ACTIVE_SESSION_STATES))).scalar() or 0)

    def _load_notebook(self, principal: Principal, body: SessionCreate, ws: dict[str, Any]) -> dict[str, Any]:
        items = self.ctx.service("items")
        item = items.get(body.notebook_id)
        if item["workspace_id"] != ws["id"]:
            raise NotFound(f"item {body.notebook_id} does not belong to workspace {ws['id']}")
        principal.require("item.read", workspace_id=ws["id"], item_id=item["id"])
        item = dict(item)
        item["_settings"] = (item.get("definition") or {}).get("settings") or {}
        return item

    def _place(self, session_id: str, ws_id: str, engine: str, res: ResourceRequest, runtime_id: str, placement: dict[str, Any], provider: str, now: float,
               flexible: bool = False) -> Any:
        if self.scheduler is None:
            raise Unavailable("the scheduler is not running")
        inp = self.scheduler.build_inputs(now, with_queued=False, extra_workspaces={ws_id})
        jv = JobView(id=session_id, workspace_id=ws_id, queue=INTERACTIVE_QUEUE, priority=100, queued_at=now, runtime_id=runtime_id, engine=engine, driver_cores=res.driver_cores,
                     driver_memory_mb=res.driver_memory_mb, executor_cores=res.executor_cores, executor_memory_mb=res.executor_memory_mb, min_executors=res.min_executors,
                     max_executors=res.max_executors, max_cores=res.max_cores, dynamic_allocation=res.dynamic_allocation, pool=placement.get("pool"),
                     labels={str(k): str(v) for k, v in (placement.get("labels") or {}).items()}, node=placement.get("node"), provider=provider, flexible=flexible)
        result = plan([jv], inp["nodes"], inp["clusters"], inp["queues"], inp["running"], inp["pools"], now=now, cfg=self.scheduler.planner_config(),
                      workspace_quotas=inp["ws_quota"], runtimes_per_node=inp["sharing"])
        if result.placements:
            return result.placements[0]
        b: Blocked | None = result.blocked[0] if result.blocked else None
        reason = f"{b.code}: {b.reason}" if b else "no placement was possible"
        if b and b.code in ("resources", "no_runtime", "no_node", "constraints", "quota"):
            raise Unavailable(f"the session cannot be started right now ({reason})", details={"code": b.code},
                              remediation="free resources (close sessions), enrol or uncordon a node, install the runtime, or request less")
        raise ResourceUnsatisfiable(reason)

    # ================================================================== kernel start (pool thread)
    def _start_kernel(self, session_id: str) -> None:
        try:
            row = self.get_row(session_id)
            if row["state"] != "starting":
                return
            spec_doc = row["spec"] or {}
            provider = self.providers.get(row["provider"])
            node = NodeInfo.from_row(self.registry.get_worker(row["worker_id"]))
            if node.state not in ("online", "degraded"):
                raise WorkerOffline(f"node '{node.name}' is not online")
            runtime_row = self.runtimes.get_row(row["runtime_id"])
            spark_block = None
            if row["engine"] == "spark" and getattr(provider, "prepares_spark", True):
                pseudo_job = {"id": row["id"], "workspace_id": row["workspace_id"], "engine": row["engine"], "kind": "session", "resources_request": spec_doc.get("resources_request") or {}}
                spark_block = provider.prepare(PrepareRequest(job=pseudo_job, placement=row["resources"] or {}, runtime=runtime_row, spark_conf=dict(spec_doc.get("spark_conf") or {}),
                                                              app_name=f"olcf-session-{row['id']}", node=node))
            token, token_id = self._mint_token(row)
            spec = self._build_spec(row, spark_block, token)
            cluster_id = (spark_block or {}).get("cluster_id")
            self._update(session_id, token_id=token_id, cluster_id=cluster_id)
            g = row["resources"] or {}
            payload = {"session_id": session_id, "spec": spec, "runtime_id": row["runtime_id"], "resources": {"cores": g.get("cores") or 1, "memory_mb": g.get("memory_mb") or 1024},
                       "idle_timeout_seconds": int(spec_doc.get("idle_timeout_seconds") or 3600) + 120, "eager_spark": bool(spec_doc.get("eager_spark", True) and row["engine"] == "spark")}
            self.hub.request(row["worker_id"], "start_kernel", payload, timeout=float(self._s("compute.session_start_timeout_seconds", 180)))
            # the agent reports 'idle' via kernel_event; make sure a missed event cannot leave the session in 'starting'
            fresh = self.get_row(session_id)
            if fresh["state"] == "starting" and age_seconds(fresh["created_at"]) is not None:
                self._update(session_id, state="idle", state_reason="kernel ready")
                self._publish("session.started", {**fresh, "state": "idle"})
        except Exception as exc:  # noqa: BLE001
            msg = redact_text(exc.message if isinstance(exc, OlcfError) else f"{type(exc).__name__}: {exc}")
            if not isinstance(exc, OlcfError):
                log.exception("starting session %s failed", session_id)
            row = self._row_or_none(session_id)
            if row is not None and row["state"] in ("idle", "busy") and isinstance(exc, (WorkerOffline, WorkerTimeout)):
                # The agent announces `idle` (kernel ready) *before* it answers start_kernel. A channel that dies in between loses only the answer: the
                # kernel is up, so keep the session; the next hello reconciles it (kernel reported -> stays, kernel missing -> finished there).
                log.warning("session %s: lost the answer to start_kernel after the kernel reported ready (%s); keeping the session", session_id, msg)
            elif row is not None and row["state"] != "dead":
                self.finish(row, f"the kernel could not be started: {msg}", failed=True)
        finally:
            with self._cond:
                self._starting.discard(session_id)

    def _mint_token(self, row: dict[str, Any]) -> tuple[str, str]:
        ttl = int((row["spec"] or {}).get("idle_timeout_seconds") or 3600) + 86400
        pid = SYSTEM_PRINCIPAL.id if row["hidden"] else (row["owner_id"] or SYSTEM_PRINCIPAL.id)
        cred = (row["spec"] or {}).get("credential")
        if not isinstance(cred, dict) or cred.get("workspace_id") != row["workspace_id"]:  # a session created before ceilings were persisted: fail closed to read-only
            cred = fixed_ceiling(LEGACY_JOB_PERMISSIONS, row["workspace_id"])
        plaintext, rec = self.ctx.tokens.create_derived(pid, cred, name=f"session {row['id']}", ttl=ttl, binding={"session_id": row["id"]})
        return plaintext, rec["id"]

    def _build_spec(self, row: dict[str, Any], spark_block: dict[str, Any] | None, token: str) -> dict[str, Any]:
        """Kernel spec = the job spec of CONTRACTS 4.3 (kind ``session``) built by the same code path as batch jobs (storage conf, secrets, lakehouses, runtime)."""
        d = dict(row["spec"] or {})
        pseudo = {"id": row["id"], "attempt": 1, "kind": "session", "engine": row["engine"], "workspace_id": row["workspace_id"], "name": d.get("name"), "source_path": None,
                  "source_hash": None, "parameters": {}, "runtime_id": row["runtime_id"], "runtime_fingerprint": (self.runtimes.get_row(row["runtime_id"]) or {}).get("fingerprint"),
                  "origin_kind": "ui", "origin": {"session": True}, "item_id": row["notebook_id"], "parent_run_id": None, "queue": INTERACTIVE_QUEUE, "submitted_by": row["owner_id"],
                  "spec": d}
        spec = self.jobs._build_spec(pseudo, spark_block, token)  # noqa: SLF001
        spec["context"]["session_id"] = row["id"]
        spec["limits"]["timeout_seconds"] = 0
        return spec

    # ================================================================== kernel events (hub listener threads)
    def _on_kernel_event(self, worker_id: str, msg: P.KernelEventMsg, frame: P.Frame) -> None:  # noqa: ARG002
        row = self._row_or_none(msg.session_id)
        if row is None:
            return
        if row["worker_id"] != worker_id:
            log.warning("ignoring kernel_event for %s from %s (kernel lives on %s)", msg.session_id, worker_id, row["worker_id"])
            return
        try:
            self.handle_kernel_event(row, msg.channel, msg.msg_type or "", msg.content, msg.parent_msg_id)
        except Exception:  # noqa: BLE001
            log.exception("kernel_event for %s could not be processed", msg.session_id)

    def handle_kernel_event(self, row: dict[str, Any], channel: str, mtype: str, content: dict[str, Any], parent: str | None) -> None:
        sid, cell = row["id"], cell_of(parent)
        if channel == "status":
            if mtype == "status":
                self._on_status(row, str(content.get("state") or ""), str(content.get("detail") or ""), cell)
            elif mtype == "spark":
                self._on_spark(row, content)
            elif content.get("event") in lineage_events.RUNNER_EVENTS:
                self._on_dataset_event(row, str(content["event"]), {k: v for k, v in content.items() if k != "event"})
            else:
                self.append(sid, "status", cell, {"event": content.get("event") or mtype, **{k: v for k, v in content.items() if k != "event"}})
        elif channel == "iopub":
            if mtype == "clear_output":
                self.append(sid, "clear", cell, {"wait": bool(content.get("wait"))})
            else:
                out = to_output(mtype, content)
                if out is not None:
                    self.append(sid, "output", cell, {"output": out})
        elif channel == "shell" and mtype == "execute_reply":
            ec = content.get("execution_count")
            if isinstance(ec, int):
                self._update(sid, execution_count=max(int(row["execution_count"] or 0), ec), last_activity_at=now_iso())
            self.append(sid, "reply", cell, {k: content.get(k) for k in ("status", "execution_count", "ename", "evalue", "traceback") if content.get(k) is not None})

    def _on_dataset_event(self, row: dict[str, Any], event: str, data: dict[str, Any]) -> None:
        """A kernel reported what the user's code read / wrote / ran (``dataset_read`` ...): forwarded as the platform ``dataset.*`` event of the notebook.

        Hidden SQL-pool sessions are skipped (the SQL service publishes its own events for those statements). Never raises.
        """
        if row.get("hidden") or row.get("kind") == "sql":
            return
        pseudo = {"kind": "session", "id": row["id"], "workspace_id": row["workspace_id"], "item_id": row.get("notebook_id"), "name": (row.get("spec") or {}).get("name"),
                  "engine": row.get("engine"), "spec": row.get("spec") or {}}
        self.jobs.forward_dataset_event(pseudo, event, data)

    def _on_status(self, row: dict[str, Any], state: str, detail: str, cell: str | None) -> None:
        if state not in ("starting", "idle", "busy", "dead"):
            return
        sid = row["id"]
        if state == "dead":
            self.append(sid, "status", cell, {"state": "dead", "detail": detail})
            self.finish(row, detail or "the kernel died", failed=False)
            return
        prev = row["state"]
        vals: dict[str, Any] = {"state": state, "state_reason": detail or None}
        if state in ("idle", "busy"):
            vals["last_activity_at"] = now_iso()
        changed = self._update(sid, **vals)
        if changed:
            self.append(sid, "status", cell, {"state": state, "detail": detail})
            if prev == "starting" and state == "idle":
                self._publish("session.started", {**row, "state": "idle"})
            elif prev != state and state == "starting":
                self._publish("session.restarting", {**row, "state": state})

    def _on_spark(self, row: dict[str, Any], content: dict[str, Any]) -> None:
        sid, app_id = row["id"], content.get("application_id")
        if not app_id:
            return
        info = {k: content.get(k) for k in ("application_id", "ui_url", "app_name", "master")}
        self._update(sid, spark=info)
        now = now_iso()
        with self.db.begin() as conn:
            cluster_id = row.get("cluster_id") or ""
            cur = fetch_one(conn, select(C.spark_apps).where(C.spark_apps.c.application_id == str(app_id), C.spark_apps.c.cluster_id == cluster_id))
            vals = {"session_id": sid, "workspace_id": row["workspace_id"], "name": content.get("app_name") or f"olcf-session-{sid}", "driver_worker_id": row["worker_id"],
                    "state": "RUNNING", "updated_at": now, "last_seen_at": now}
            if content.get("ui_url"):
                vals["ui_url"] = str(content["ui_url"])
            if cur is None:
                insert_row(conn, C.spark_apps, {"id": new_id("sap"), "application_id": str(app_id), "cluster_id": cluster_id, "executors": [], "started_at": now, "created_at": now,
                                                "ui_url": None, **vals})
            else:
                conn.execute(update(C.spark_apps).where(C.spark_apps.c.id == cur["id"]).values(**vals))
        self.append(sid, "spark", None, {"application_id": app_id, "ui_url": None, "app_name": content.get("app_name")})
        self._publish("session.spark_started", row, application_id=app_id)

    # ================================================================== event relay table
    def append(self, session_id: str, kind: str, cell_id: str | None, data: dict[str, Any]) -> None:
        with self.db.begin() as conn:
            conn.execute(C.session_events.insert().values(session_id=session_id, ts=now_iso(), kind=kind, cell_id=cell_id, data=redact_obj(data)))
        with self._cond:
            self._epoch += 1
            self._cond.notify_all()
            for loop, ev in list(self._waiters.get(session_id, ())):
                try:
                    loop.call_soon_threadsafe(ev.set)
                except RuntimeError:  # loop already closed
                    self._waiters.get(session_id, set()).discard((loop, ev))
            n = self._since_prune.get(session_id, 0) + 1
            self._since_prune[session_id] = 0 if n >= 50 else n
        if n >= 50:
            self.prune(session_id)

    def prune(self, session_id: str) -> int:
        """Keep the newest ``compute.session_replay_events`` output-like events (and everything newer than the cutoff) per session."""
        keep = max(50, int(self._s("compute.session_replay_events", 500)))
        t = C.session_events.c
        with self.db.begin() as conn:
            rows = conn.execute(select(t.seq).where(t.session_id == session_id, t.kind.in_(_OUTPUT_KINDS)).order_by(t.seq.desc()).offset(keep).limit(1)).first()
            if not rows:
                return 0
            return int(conn.execute(delete(C.session_events).where(t.session_id == session_id, t.seq <= rows[0])).rowcount or 0)

    def events_after(self, session_id: str, after_seq: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        t = C.session_events.c
        with self.db.connect() as conn:
            return fetch_all(conn, select(C.session_events).where(t.session_id == session_id, t.seq > after_seq).order_by(t.seq.asc()).limit(limit))

    def head_seq(self, session_id: str) -> int:
        with self.db.connect() as conn:
            return int(conn.execute(select(func.max(C.session_events.c.seq)).where(C.session_events.c.session_id == session_id)).scalar() or 0)

    def replay(self, session_id: str, upto: int | None = None) -> list[dict[str, Any]]:
        """The newest output-like events (``seq <= upto``) in chronological order (browser reconnect)."""
        keep = max(50, int(self._s("compute.session_replay_events", 500)))
        t = C.session_events.c
        stmt = select(C.session_events).where(t.session_id == session_id, t.kind.in_(("output", "reply", "clear")))
        if upto is not None:
            stmt = stmt.where(t.seq <= upto)
        with self.db.connect() as conn:
            rows = fetch_all(conn, stmt.order_by(t.seq.desc()).limit(keep))
        return list(reversed(rows))

    def subscribe(self, session_id: str, loop: asyncio.AbstractEventLoop, event: asyncio.Event) -> Callable[[], None]:
        """Wake an asyncio waiter when this replica appends an event for the session (events appended by other replicas are found by the caller's poll)."""
        entry = (loop, event)
        with self._cond:
            self._waiters.setdefault(session_id, set()).add(entry)

        def unsubscribe() -> None:
            with self._cond:
                self._waiters.get(session_id, set()).discard(entry)
                if not self._waiters.get(session_id):
                    self._waiters.pop(session_id, None)

        return unsubscribe

    def wait_for_events(self, last_epoch: int, timeout: float) -> int:
        """Block until a local event arrived (or ``timeout``); returns the current epoch. Remote replicas are covered by the caller's poll interval."""
        with self._cond:
            if self._epoch == last_epoch:
                self._cond.wait(timeout)
            return self._epoch

    @property
    def epoch(self) -> int:
        return self._epoch

    # ================================================================== commands
    def _cmd(self, row: dict[str, Any], type_: str, payload: dict[str, Any], timeout: float = 30.0) -> Any:
        if row["state"] == "dead":
            raise Conflict("the session has ended", remediation="start a new session")
        if not row["worker_id"]:
            raise Conflict("the session has no kernel node")
        return self.hub.request(row["worker_id"], type_, {"session_id": row["id"], **payload}, timeout=timeout)

    def touch(self, session_id: str) -> None:
        self._update(session_id, last_activity_at=now_iso(), updated_at=now_iso())

    def execute(self, principal: Principal | None, session_id: str, cell_id: str, code: str) -> dict[str, Any]:
        row = self.authorize(principal, session_id) if principal is not None else self.get_row(session_id)
        if row["state"] == "starting":
            raise Conflict("the kernel is still starting", remediation="wait for the 'idle' status")
        if row["state"] != "idle" and row["state"] != "busy":
            raise Conflict(f"the session is {row['state']}")
        if len(code.encode("utf-8")) > 4 * 1024 * 1024:
            raise BadRequest("cell code is larger than 4 MiB")
        msg_id = make_msg_id(cell_id)
        self._cmd(row, "kernel_execute", {"msg_id": msg_id, "code": code})
        self.touch(session_id)
        return {"session_id": session_id, "cell_id": cell_id, "msg_id": msg_id}

    def interrupt(self, principal: Principal, session_id: str) -> dict[str, Any]:
        row = self.authorize(principal, session_id)
        self._cmd(row, "kernel_interrupt", {})
        self.touch(session_id)
        self.ctx.audit.record(principal, "session.interrupt", resource_type="session", resource_id=session_id, workspace_id=row["workspace_id"])
        return self.public(self.get_row(session_id))

    def restart(self, principal: Principal, session_id: str) -> dict[str, Any]:
        row = self.authorize(principal, session_id)
        self._update(session_id, state="starting", state_reason="restarting the kernel", spark=None)
        self.append(session_id, "status", None, {"state": "starting", "detail": "restarting the kernel"})
        try:
            self._cmd(row, "kernel_restart", {}, timeout=float(self._s("compute.session_start_timeout_seconds", 180)))
        except OlcfError as exc:
            self.finish(self.get_row(session_id), f"restart failed: {exc.message}", failed=True)
            raise
        fresh = self.get_row(session_id)
        if fresh["state"] == "starting":
            self._update(session_id, state="idle", state_reason="kernel restarted", last_activity_at=now_iso())
        self.ctx.audit.record(principal, "session.restart", resource_type="session", resource_id=session_id, workspace_id=row["workspace_id"])
        return self.public(self.get_row(session_id))

    def complete(self, principal: Principal, session_id: str, code: str, cursor: int | None) -> dict[str, Any]:
        row = self.authorize(principal, session_id)
        if row["state"] != "idle" and row["state"] != "busy":
            raise Conflict(f"the session is {row['state']}")
        return self._cmd(row, "kernel_complete", {"code": code, "cursor": len(code) if cursor is None else int(cursor), "timeout": 10.0}, timeout=20.0) or {}

    def delete(self, principal: Principal, session_id: str) -> dict[str, Any]:
        row = self.authorize(principal, session_id)
        self.ctx.audit.record(principal, "session.deleted", resource_type="session", resource_id=session_id, workspace_id=row["workspace_id"])
        return self.shutdown(row, "closed by the user")

    def shutdown(self, row: dict[str, Any], reason: str) -> dict[str, Any]:
        if row["state"] != "dead" and row["worker_id"]:
            try:
                self.hub.request(row["worker_id"], "kernel_shutdown", {"session_id": row["id"]}, timeout=15.0)
            except OlcfError as exc:
                log.info("kernel_shutdown for %s failed (%s); the kernel is reclaimed when the agent reports", row["id"], exc.message)
        self.finish(self.get_row(row["id"]), reason, failed=False)
        return self.public(self.get_row(row["id"]))

    def finish(self, row: dict[str, Any], reason: str, *, failed: bool) -> None:
        """Terminal transition (idempotent): release resources, revoke the token, finish the Spark application, publish ``session.stopped``."""
        sid = row["id"]
        now = now_iso()
        with self.db.begin() as conn:
            n = conn.execute(update(C.sessions).where(C.sessions.c.id == sid, C.sessions.c.state != "dead")
                             .values(state="dead", state_reason=reason[:500], stopped_at=now, updated_at=now)).rowcount
            if n:
                conn.execute(update(C.spark_apps).where(C.spark_apps.c.session_id == sid, C.spark_apps.c.state.in_(("RUNNING", "WAITING")))
                             .values(state="FINISHED", finished_at=now, updated_at=now))
        if not n:
            return
        fresh = self.get_row(sid)
        if fresh.get("token_id"):
            try:
                self.ctx.tokens.revoke(fresh["token_id"])
            except Exception:  # noqa: BLE001
                log.debug("session token revoke failed", exc_info=True)
        self.append(sid, "status", None, {"state": "dead", "detail": reason})
        self._publish("session.stopped", fresh, reason=reason, failed=failed)
        with self._cond:
            self._since_prune.pop(sid, None)
        if self.scheduler is not None:
            self.scheduler.wake()

    # ================================================================== reconcile (hello)
    def _on_hello(self, worker: dict[str, Any], hello: P.Hello) -> None:
        wid = worker["id"]
        reported = {k.session_id: k for k in hello.kernels}
        with self.db.connect() as conn:
            mine = fetch_all(conn, select(C.sessions).where(C.sessions.c.worker_id == wid, C.sessions.c.state.in_(ACTIVE_SESSION_STATES)))
        known = {r["id"] for r in mine}
        for r in mine:
            k = reported.get(r["id"])
            if k is None:
                if r["id"] in self._starting:
                    continue  # the start command is in flight
                self.finish(r, "the kernel is gone (the node or its agent restarted)", failed=True)
            elif k.state in ("dead", "exited"):
                self.finish(r, "the kernel is dead on the node", failed=True)
        for sid in reported:
            if sid not in known:
                row = self._row_or_none(sid)
                if row is None or row["state"] == "dead" or row["worker_id"] != wid:
                    self._pool.submit(self._shutdown_orphan, wid, sid)  # never block the hello handler on the agent

    def _shutdown_orphan(self, wid: str, sid: str) -> None:
        try:
            self.hub.request(wid, "kernel_shutdown", {"session_id": sid}, timeout=15.0)
            log.info("shut down orphan kernel %s on %s", sid, wid)
        except OlcfError:
            log.warning("could not shut down orphan kernel %s on %s", sid, wid)

    # ================================================================== maintenance (leader tick)
    def maintain(self) -> None:
        now = time.time()
        with self.db.connect() as conn:
            active = fetch_all(conn, select(C.sessions).where(C.sessions.c.state.in_(ACTIVE_SESSION_STATES)))
        start_to = float(self._s("compute.session_start_timeout_seconds", 180)) + 60
        grace = float(self._s("compute.worker_lost_grace_seconds", 180))
        workers: dict[str, dict[str, Any] | None] = {}
        for r in active:
            try:
                age_active = age_seconds(r["last_activity_at"]) or 0.0
                idle_limit = float((r.get("spec") or {}).get("idle_timeout_seconds") or self._s("compute.session_idle_timeout_seconds", 3600))
                if r["state"] == "idle" and age_active > idle_limit:
                    self.shutdown(r, f"idle for more than {int(idle_limit)} seconds")
                    continue
                if r["state"] == "starting" and r["id"] not in self._starting and (age_seconds(r["updated_at"]) or 0.0) > start_to:
                    self.finish(r, "the kernel did not start (controller restarted or agent unresponsive)", failed=True)
                    continue
                w = self._worker_cached(workers, r["worker_id"])
                if w is None:
                    self.finish(r, "the kernel node no longer exists", failed=True)
                elif w["state"] == "revoked":
                    self.finish(r, "the kernel node was revoked", failed=True)
                elif w["state"] == "offline" and (age_seconds(w.get("last_seen_at")) or 1e9) > grace:
                    self.finish(r, f"the kernel node has been offline for more than {int(grace)} seconds", failed=True)
            except Exception:  # noqa: BLE001
                log.exception("session maintenance for %s failed", r["id"])
        if now - getattr(self, "_last_gc", 0.0) > 300:
            self._last_gc = now
            self._gc()

    def _worker_cached(self, cache: dict[str, dict[str, Any] | None], worker_id: str | None) -> dict[str, Any] | None:
        if not worker_id:
            return None
        if worker_id not in cache:
            try:
                cache[worker_id] = self.registry.get_worker(worker_id)
            except NotFound:
                cache[worker_id] = None
        return cache[worker_id]

    def _gc(self) -> None:
        """Drop relay rows of long-dead sessions and the session rows themselves after the retention period."""
        keep_days = float(self._s("compute.session_retention_days", 7))
        cutoff_events = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
        cutoff_rows = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - keep_days * 86400))
        with self.db.begin() as conn:
            old = [r[0] for r in conn.execute(select(C.sessions.c.id).where(C.sessions.c.state == "dead", C.sessions.c.stopped_at < cutoff_events).limit(500)).all()]
            if old:
                conn.execute(delete(C.session_events).where(C.session_events.c.session_id.in_(old)))
            conn.execute(delete(C.sessions).where(C.sessions.c.state == "dead", C.sessions.c.stopped_at < cutoff_rows))

    # ================================================================== SQL through pooled hidden sessions
    def _sql_lock(self, key: str) -> threading.Lock:
        with self._sql_guard:
            return self._sql_locks.setdefault(key, threading.Lock())

    def run_spark_sql(self, principal: Principal, workspace_id: str, sql: str, *, lakehouse: str | LakehouseRef | None = None, limit: int = 1000, timeout: float = 120.0,
                      runtime: RuntimeRequest | None = None) -> dict[str, Any]:
        """Run ``sql`` with Spark in a pooled hidden session and return ``{columns, rows, row_count, truncated, elapsed_ms, session_id, application_id}``.

        Authorisation: ``sql.execute`` in the workspace (and ``sql.write`` for statements that are not plainly read-only) plus ``lakehouse.read`` on the lakehouse.
        At most ``limit`` (<= 100000) rows are materialised, the statement is aborted (kernel interrupt) after ``timeout`` seconds.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise BadRequest("sql must be a non-empty string")
        if len(sql) > 1_000_000:
            raise BadRequest("sql is larger than 1 MB")
        limit = max(1, min(int(limit), MAX_SQL_ROWS))
        ws = self.jobs._workspace(workspace_id)  # noqa: SLF001
        principal.require("sql.execute", workspace_id=ws["id"])
        # F02/C01: whoever calls, the kernel is shared. Host capabilities (reflection, script transformation, file queries, ${..} substitution, unapproved or qualified functions)
        # are refused before any session is created, and so is any text that is not exactly one parsed, supported statement - there is no unparsed fallback for a writer.
        from olcf.sql import guard

        stmt = guard.check_spark_sql(sql, strict=False)  # one fully parsed, supported statement for every caller; host capabilities and unapproved functions refused (C01)
        if not stmt.read_only:
            principal.require("sql.write", workspace_id=ws["id"])  # a viewer gets 403 for a statement that parses as a write
        ref = LakehouseRef(lakehouse_id=lakehouse) if isinstance(lakehouse, str) and lakehouse.startswith("itm_") else (LakehouseRef(name=lakehouse) if isinstance(lakehouse, str) else lakehouse)
        rt_req = runtime or RuntimeRequest()
        selection = self.runtimes.resolve(rt_req, workspace_id=ws["id"])
        lake_key = ""
        if ref is not None:
            sub = JobSubmission(kind=JobKind.SQL, workspace_id=ws["id"], default_lakehouse=ref)
            lakes = self.jobs._resolve_lakehouses(principal, sub, ws, ref)  # noqa: SLF001  (checks lakehouse.read/write for the *caller*)
            lake_key = (lakes["default"] or {}).get("id") or ""
        key = f"{ws['id']}|{lake_key}|{selection.runtime_id}"
        deadline = time.monotonic() + float(timeout)
        with self._sql_lock(key):
            row = self._pooled_session(principal, ws, ref, rt_req, key, deadline)
            return self._exec_sql(row, sql, limit, deadline)

    def _pooled_session(self, principal: Principal, ws: dict[str, Any], ref: LakehouseRef | None, rt_req: RuntimeRequest, key: str, deadline: float) -> dict[str, Any]:
        t = C.sessions.c
        for _attempt in range(2):
            with self.db.connect() as conn:
                row = fetch_one(conn, select(C.sessions).where(t.pool_key == key, t.hidden.is_(True), t.state.in_(ACTIVE_SESSION_STATES)).order_by(t.created_at.desc()).limit(1))
            if row is None:
                with self.registry.db_lock(f"sql-session:{key}"[:120], ttl=60.0, wait=60.0):
                    with self.db.connect() as conn:
                        row = fetch_one(conn, select(C.sessions).where(t.pool_key == key, t.hidden.is_(True), t.state.in_(ACTIVE_SESSION_STATES)).limit(1))
                    if row is None:
                        body = SessionCreate(workspace_id=ws["id"], engine="spark", runtime=rt_req, default_lakehouse=ref, name="sql", eager_spark=True)
                        row = self.create(principal, body, hidden=True, pool_key=key, kind="sql", owner_id=SYSTEM_PRINCIPAL.id, authorized=True)
            while row["state"] == "starting":
                if time.monotonic() > deadline:
                    raise Unavailable("the SQL session is still starting; retry shortly")
                time.sleep(0.2)
                row = self.get_row(row["id"])
            if row["state"] in ("idle", "busy"):
                return row
        raise Unavailable("no SQL session could be started", remediation=(row.get("state_reason") if row else None) or "check worker and runtime health")

    def _exec_sql(self, row: dict[str, Any], sql: str, limit: int, deadline: float) -> dict[str, Any]:
        sid = row["id"]
        cell = f"sql-{uuid.uuid4().hex[:10]}"
        code = _SQL_TEMPLATE.format(sql_literal=json.dumps(json.dumps(sql)), limit_plus=limit + 1, mark=SQL_MARK)
        start_seq = self.head_seq(sid)
        t0 = time.monotonic()
        self.execute(None, sid, cell, code)
        seq, result, error, reply = start_seq, None, None, None
        text_parts: list[str] = []
        epoch = self.epoch
        while reply is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                try:
                    self._cmd(self.get_row(sid), "kernel_interrupt", {})
                except OlcfError:
                    pass
                raise Unavailable("the SQL statement did not finish within the timeout", remediation="raise the timeout or reduce the amount of data scanned")
            for ev in self.events_after(sid, seq):
                seq = ev["seq"]
                if ev["kind"] == "status" and (ev["data"] or {}).get("state") == "dead":
                    raise Unavailable("the SQL session died while the statement was running", remediation=(ev["data"] or {}).get("detail"))
                if ev["cell_id"] != cell:
                    continue
                if ev["kind"] == "output":
                    out = (ev["data"] or {}).get("output") or {}
                    if out.get("output_type") == "stream":
                        text_parts.append(out.get("text") or "")
                    elif out.get("output_type") == "error":
                        error = out
                elif ev["kind"] == "reply":
                    reply = ev["data"] or {}
            if reply is None:
                epoch = self.wait_for_events(epoch, min(0.25, max(0.01, remaining)))
        if error is not None or (reply or {}).get("status") not in (None, "ok"):
            err = error or reply or {}
            name, value = str(err.get("ename") or "SparkError"), str(err.get("evalue") or "")
            raise BadRequest(redact_text(f"{name}: {value}")[:2000], details={"session_id": sid}, remediation="check the SQL text, table names and the lakehouse the query runs against")
        blob = "".join(text_parts)
        i = blob.rfind(SQL_MARK)
        if i < 0:
            raise OlcfError("the SQL session returned no result")
        try:
            result = json.loads(blob[i + len(SQL_MARK):].splitlines()[0])
        except (ValueError, IndexError) as exc:
            raise OlcfError("the SQL session returned an unreadable result") from exc
        rows = result["rows"]
        truncated = len(rows) > limit
        spark = row.get("spark") or {}
        self.touch(sid)
        return {"columns": result["columns"], "rows": rows[:limit], "row_count": min(len(rows), limit), "truncated": truncated, "elapsed_ms": int((time.monotonic() - t0) * 1000),
                "session_id": sid, "application_id": spark.get("application_id"), "limit": limit}

    # ================================================================== lifecycle
    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

