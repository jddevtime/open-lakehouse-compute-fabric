# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job service: submission, the persisted state machine, dispatch, agent callbacks, cancellation, timeouts and recovery.

Lifecycle (CONTRACTS 4.1, spec 33)::

    accepted -> validating -> queued -> provisioning -> running -> succeeded | failed | cancelled | timed_out
                                  ^____________|______________|        (automatic retry, see compute.failures)

Every transition is a compare-and-set ``UPDATE ... WHERE state = <expected>`` plus a ``job_events`` row in the same transaction, followed by a
``job.state_changed`` event ``{job_id, from, to, reason}``. Because the database is the single source of truth, any API replica may
handle an agent callback and a new scheduler leader can continue where the old one stopped (:meth:`JobService.recover`).

Division of labour
------------------
* :class:`JobService` (this module) owns *what happens to a job*: validation, persistence, spec generation, launch, callbacks, retry, cancel.
* ``scheduler.py`` owns *when and where*: it calls :meth:`claim` (queued -> provisioning) for each placement the planner produces,
  :meth:`launch` to hand the job to its provider, and :meth:`maintain` for timeouts, lost workers and stuck states.
* Providers (``providers/``) own *how* an application is started on a concrete system.

Safety rules
------------
* A job is never executed twice: ``run_job`` is keyed by ``(job_id, attempt)`` on the agent, and automatic replay obeys
  :func:`olcf.compute.failures.retry_decision` (idempotent or never-started, and agent-confirmed gone after infrastructure loss).
* Secrets (storage credentials, ``secret_refs``) are resolved just-in-time inside :meth:`launch`, placed only in the spec sent to the
  agent and never persisted, logged or returned by an API. The job-scoped token is revoked when the attempt ends.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from olcf.compute import artifacts as artifacts_mod
from olcf.compute import lineage_events
from olcf.compute import protocol as P
from olcf.compute import schema as C
from olcf.compute.errors import (
    ProviderError,
    ResourceUnsatisfiable,
    WorkerCommandFailed,
    WorkerOffline,
    WorkerTimeout,
)
from olcf.compute.failures import classify_failure, retry_decision
from olcf.compute.hub import WorkerHub
from olcf.compute.logstore import LogStore
from olcf.compute.planner import JobView, PlannerConfig, infeasible_reason
from olcf.compute.providers import ProviderManager
from olcf.compute.providers.base import (
    TERMINAL_APP_STATES,
    AppHandle,
    LogsPage,
    PrepareRequest,
    SubmitRequest,
)
from olcf.compute.registry import ACTIVE_JOB_STATES, ComputeRegistry, NodeInfo
from olcf.compute.runtimes import RuntimeCatalog, RuntimeSelection, filter_spark_conf
from olcf.core import dataset_events as dsev
from olcf.core.context import AppContext
from olcf.core.errors import (
    BadRequest,
    CompatibilityBlocked,
    Conflict,
    FailureCategory,
    Forbidden,
    NotFound,
    OlcfError,
    PolicyDenied,
    TooManyRequests,
    Unavailable,
    Unsupported,
    ValidationFailed,
)
from olcf.core.hostruntime import HOST_RUNTIME_ID
from olcf.core.ids import new_id
from olcf.core.models import (
    FailureInfo,
    JobKind,
    JobRecord,
    JobSource,
    JobSubmission,
    LakehouseRef,
    Origin,
    OriginKind,
)
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.states import TERMINAL_STATES, JobState, check_transition
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, fetch_one, insert_row, new_etag, paginate
from olcf.security.rbac import LEGACY_JOB_PERMISSIONS, SYSTEM_PRINCIPAL, Principal, fixed_ceiling

log = logging.getLogger("olcf.compute.jobs")

SYSTEM_WORKSPACE = "wks_system"  # org-level platform jobs (spark smoke test): only org-wide roles can see them
TERMINAL = tuple(s.value for s in TERMINAL_STATES)
PRE_RUN = ("accepted", "validating", "queued")
MAX_INLINE_SOURCE = 16 * 1024 * 1024
MAX_EVENT_DATA_BYTES = 16 * 1024
MAX_RESULT_VALUE_BYTES = 256 * 1024
SOURCE_NAMES = {"ipynb": "notebook.ipynb", "fabric_git_source": "notebook-content.py", "py": "main.py", "sql": "query.sql", "scala": "main.scala", "jar": "app.jar", "script": "script.sh"}
KIND_FORMATS = {
    JobKind.NOTEBOOK: {"ipynb", "fabric_git_source", "py"}, JobKind.CAPSULE: {"ipynb", "fabric_git_source"}, JobKind.SQL: {"sql"},
    JobKind.SPARK_JOB: {"py", "jar", "scala"}, JobKind.SCRIPT: {"script", "py"},
}
FABRIC_MARKERS = re.compile(r"notebookutils|mssparkutils|%run\b|%%configure|spark\.fabric|onelake\.dfs\.fabric|META\s*\{\s*\"kernel_info\"|\bsempy\b|synapse\.ml\.fabric", re.I)


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _like_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


_QUEUE_HINTS = {
    "paused": "resume the queue (Compute > Queues and pools, or POST /v1/queues/<name>/resume; needs the QueueAdmin role)",
    "quota": "wait for running jobs in the queue/workspace to finish, or raise the quota",
    "no_runtime": "see the message: install the runtime or use a Python-engine notebook (host Python, no install needed)",
    "no_node": "no worker is connected: `olcf serve` embeds one (unless --no-embedded-worker), or enrol a machine with `olcf worker enroll-token` and `olcf-worker run`; check `olcf worker list`",
    "constraints": "relax the pool/label/node placement constraints of the request",
    "resources": "wait for running jobs to free capacity, or reduce the cores/memory requested",
    "infeasible": "reduce the resource request or enrol a larger worker; the job fails after the grace period",
    "held": "a higher-priority job that keeps being skipped is being served first; this job runs right after it",
    "backoff": "the job is retrying after a failure; it runs again when the back-off elapses",
}


def queue_reason(row: dict[str, Any]) -> dict[str, Any] | None:
    """Why a *queued* job is not running yet, as ``{code, message, remediation}`` (``None`` once it is running/terminal or before the scheduler has looked at it).

    ``code`` is one of paused, quota, no_runtime, no_node, constraints, resources, infeasible, held, backoff; it is derived from ``wait_reason`` (``"<code>: <sentence>"``).
    """
    if row.get("state") != "queued" or not row.get("wait_reason"):
        return None
    code, _, msg = str(row["wait_reason"]).partition(": ")
    if not msg:
        code, msg = "waiting", str(row["wait_reason"])
    # the sentence may already carry its own remediation after the last "; " - keep it whole in `message`, add the generic one for the code
    return {"code": code, "message": msg, "remediation": _QUEUE_HINTS.get(code)}


class JobService:
    """Registered as ``jobs`` (and ``compute.jobs``). See the module docstring."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: WorkerHub, logs: LogStore, runtimes: RuntimeCatalog, providers: ProviderManager) -> None:
        self.ctx, self.registry, self.hub, self.logs, self.runtimes, self.providers = ctx, registry, hub, logs, runtimes, providers
        self.db = ctx.db
        self.jobs_dir = ctx.settings.path("jobs")
        self.wake: Callable[[], None] = lambda: None  # set by the scheduler: run a planning pass soon
        self._waiters: dict[str, threading.Event] = {}
        self._waiters_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="olcf-launch")
        self._remote_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="olcf-remote")
        self._event_lock = threading.Lock()
        #: dataset events reported by runners: per-attempt budget and forwarded / dropped counters (see :meth:`forward_dataset_event`)
        self.dataset_event_budget = lineage_events.EventBudget()
        self.dataset_event_stats = lineage_events.EventStats()
        self._launching: set[str] = set()
        self._last_slow = 0.0
        self._last_retention = 0.0
        hub.on("job_state", self._on_job_state)
        hub.on("job_event", self._on_job_event)
        hub.on_hello(self._on_hello)
        hub.job_owner_lookup = self._owner_of

    # =================================================================== settings helpers
    def _s(self, key: str, default: Any) -> Any:
        v = self.registry.setting(key, default)
        return default if v is None else v

    @property
    def grace_cancel(self) -> int:
        return int(self._s("compute.cancel_grace_seconds", 15))

    def api_url(self) -> str:
        return str(self._s("compute.api_url", "") or self.ctx.settings.base_url).rstrip("/")

    # =================================================================== reads
    def get_row(self, job_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.jobs).where(C.jobs.c.id == job_id))
        if not row:
            raise NotFound(f"job {job_id} not found")
        return row

    def _owner_of(self, job_id: str) -> str | None:
        with self.db.connect() as conn:
            r = conn.execute(select(C.jobs.c.driver_worker_id).where(C.jobs.c.id == job_id)).first()
        return r[0] if r else None

    def to_record(self, row: dict[str, Any]) -> JobRecord:
        f = row.get("failure")
        failure = None
        if isinstance(f, dict) and f.get("category"):
            known = {k: f.get(k) for k in ("category", "message", "cell", "line", "traceback", "remediation")}
            failure = FailureInfo(category=str(known["category"]), message=str(known["message"] or ""), cell=known["cell"], line=known["line"], traceback=known["traceback"],
                                  remediation=known["remediation"], details={k: v for k, v in f.items() if k not in known})
        return JobRecord(
            id=row["id"], kind=JobKind(row["kind"]), state=row["state"], workspace_id=row["workspace_id"], name=row["name"], item_id=row["item_id"],
            submitted_by=row["submitted_by"], origin=Origin(kind=OriginKind(row["origin_kind"]), details=row["origin"] or {}), queue=row["queue"], priority=row["priority"],
            attempt=row["attempt"], runtime_id=row["runtime_id"], runtime_fingerprint=row["runtime_fingerprint"], source_hash=row["source_hash"], provider=row["provider"],
            application_id=row["application_id"], driver_worker_id=row["driver_worker_id"], resources_granted=row["resources_granted"] or {}, parameters=row["parameters"] or {},
            exit_value=row["exit_value"], result=row["result"] or {}, failure=failure, parent_run_id=row["parent_run_id"], correlation_id=row["correlation_id"],
            created_at=row["created_at"], updated_at=row["updated_at"], queued_at=row["queued_at"], started_at=row["started_at"], finished_at=row["finished_at"],
            timeout_at=row["timeout_at"], cancel_requested_at=row["cancel_requested_at"])

    def get(self, job_id: str) -> JobRecord:
        return self.to_record(self.get_row(job_id))

    def public(self, row: dict[str, Any], *, detail: bool = False) -> dict[str, Any]:
        """REST representation (JobRecord plus scheduler/diagnostic fields). ``detail`` adds timeline, attempts and hashes."""
        out = self.to_record(row).model_dump(mode="json")
        out.update({"engine": row["engine"], "state_reason": row["state_reason"], "wait_reason": row["wait_reason"], "cluster_id": row["cluster_id"], "max_attempts": row["max_attempts"],
                    "retry_of": row["retry_of"], "failure_category": row["failure_category"], "timed_out": bool(row["timed_out"]), "resources_request": row["resources_request"] or {}})
        out["queue_reason"] = queue_reason(row)
        if row.get("application_id"):
            with self.db.connect() as conn:
                app = fetch_one(conn, select(C.spark_apps.c.id, C.spark_apps.c.state).where(C.spark_apps.c.job_id == row["id"]).order_by(C.spark_apps.c.created_at.desc()).limit(1))
            if app:
                out["spark_application"] = {"id": app["id"], "application_id": row["application_id"], "state": app["state"], "ui": f"/v1/spark-applications/{app['id']}/ui/"}
        if detail:
            spec = row.get("spec") or {}
            out["timeline"] = self.timeline(row["id"])
            out["attempts"] = self.attempts(row["id"])
            out["hashes"] = {"source": row["source_hash"], "runtime_fingerprint": row["runtime_fingerprint"], "spec": spec.get("spec_hash")}
            out["request"] = {"policy": spec.get("policy"), "runtime": spec.get("runtime_request"), "resources": row["resources_request"], "tags": spec.get("tags"),
                              "env_keys": sorted((spec.get("env") or {}).keys()), "secret_env_names": sorted((spec.get("secret_refs") or {}).keys()), "source": spec.get("source"),
                              "default_lakehouse": (spec.get("default_lakehouse") or {}).get("id"), "compatibility": spec.get("compatibility"),
                              "rejected_spark_conf": spec.get("rejected_spark_conf") or []}
        return redact_obj(out)

    def timeline(self, job_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.job_events).where(C.job_events.c.job_id == job_id).order_by(C.job_events.c.seq.asc()).limit(limit))
        return [{"seq": r["seq"], "ts": r["ts"], "kind": r["kind"], "from": r["from_state"], "to": r["to_state"], "reason": r["reason"], "event": r["event"], "data": r["data"]} for r in rows]

    def attempts(self, job_id: str) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.job_attempts).where(C.job_attempts.c.job_id == job_id).order_by(C.job_attempts.c.attempt.asc()))
        return [{k: r[k] for k in ("attempt", "worker_id", "cluster_id", "provider", "state", "application_id", "exit_code", "failure", "process_confirmed_gone", "created_at", "started_at", "finished_at")}
                for r in rows]

    def list(self, principal: Principal, *, workspace_id: str | None = None, state: list[str] | None = None, kind: str | None = None, item_id: str | None = None,
             origin: str | None = None, queue: str | None = None, since: str | None = None, until: str | None = None, text: str | None = None,
             parent_run_id: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Keyset-paginated listing (newest first) restricted to what ``principal`` may read. Every filter has an index."""
        t = C.jobs.c
        stmt = select(C.jobs)
        if workspace_id:
            principal.require("job.read", workspace_id=workspace_id)
            stmt = stmt.where(t.workspace_id == workspace_id)
        else:
            vis = principal.visible_workspaces("job.read")
            if vis is not None:
                if not vis:
                    return [], None
                stmt = stmt.where(t.workspace_id.in_(sorted(vis)))
        if state:
            bad = [s for s in state if s not in {e.value for e in JobState}]
            if bad:
                raise BadRequest(f"unknown job state(s): {bad}")
            stmt = stmt.where(t.state.in_(state))
        for col, val in ((t.kind, kind), (t.item_id, item_id), (t.origin_kind, origin), (t.queue, queue), (t.parent_run_id, parent_run_id)):
            if val:
                stmt = stmt.where(col == val)
        if since:
            stmt = stmt.where(t.created_at >= since)
        if until:
            stmt = stmt.where(t.created_at <= until)
        if text:
            stmt = stmt.where(or_(t.id == text, func.lower(t.name).like(f"%{_like_escape(text.lower())}%", escape="\\")))
        with self.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, C.jobs, limit=limit, cursor=cursor, descending=True)
        return rows, nxt

    def authorize(self, principal: Principal, job_id: str, perm: str = "job.read") -> dict[str, Any]:
        """Load a job and require ``perm`` on *its* workspace (never a client-supplied one)."""
        row = self.get_row(job_id)
        principal.require(perm, workspace_id=row["workspace_id"], item_id=row["item_id"])
        return row

    def wait(self, job_id: str, timeout: float = 60.0) -> JobRecord:
        """Block until the job is terminal or ``timeout`` expires (returns the latest record either way)."""
        deadline = time.monotonic() + timeout
        with self._waiters_lock:
            ev = self._waiters.setdefault(job_id, threading.Event())
        try:
            while True:
                row = self.get_row(job_id)
                if row["state"] in TERMINAL:
                    return self.to_record(row)
                left = deadline - time.monotonic()
                if left <= 0:
                    return self.to_record(row)
                ev.wait(min(0.5, left))
                ev.clear()
        finally:
            with self._waiters_lock:
                if self._waiters.get(job_id) is ev:
                    self._waiters.pop(job_id, None)

    def _notify(self, job_id: str) -> None:
        with self._waiters_lock:
            ev = self._waiters.get(job_id)
        if ev:
            ev.set()

    def logs_page(self, job_id: str, offset: int = 0, limit: int = 65536, stream: str = "combined", tail: bool = False) -> dict[str, Any]:
        if not self.logs.exists(job_id):
            row = self.get_row(job_id)
            return {"job_id": job_id, "stream": stream, "offset": 0, "next_offset": 0, "data": "", "eof": row["state"] in TERMINAL, "size": 0}
        page = self.logs.read(job_id, offset, limit, stream, tail=tail)
        out = page.to_dict()
        row = self.get_row(job_id)
        out["eof"] = bool(row["state"] in TERMINAL and page.next_offset >= self.logs.size(job_id, stream))
        out["size"] = self.logs.size(job_id, stream)
        return out

    # =================================================================== persistence primitives
    def add_event(self, job_id: str, event: str, data: dict[str, Any] | None = None, *, reason: str | None = None) -> None:
        data = redact_obj(data or {})
        if len(json.dumps(data, default=str)) > MAX_EVENT_DATA_BYTES:
            data = {"truncated": True, "keys": sorted(data)[:20]}
        with self.db.begin() as conn:
            conn.execute(C.job_events.insert().values(job_id=job_id, ts=now_iso(), kind="event", event=event[:64], reason=reason, data=data))

    def transition(self, job_id: str, to: str, reason: str = "", *, expect: tuple[str, ...] | None = None, fields: dict[str, Any] | None = None,
                   event_data: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Compare-and-set state change + ``job_events`` row + ``job.state_changed`` event.

        Returns the updated row, or ``None`` when the job is already in ``to`` or not in one of ``expect`` (a benign race lost).
        Raises :class:`InvalidTransition` for transitions the state machine forbids.
        """
        reason = redact_text(reason or "")[:500]
        for _ in range(5):
            row = self.get_row(job_id)
            cur = row["state"]
            if cur == to or (expect is not None and cur not in expect):
                return None
            check_transition(cur, to)
            now = now_iso()
            vals: dict[str, Any] = {"state": to, "state_reason": reason, "updated_at": now, **(fields or {})}
            if to in TERMINAL:
                vals.setdefault("finished_at", now)
            with self.db.begin() as conn:
                n = conn.execute(update(C.jobs).where(C.jobs.c.id == job_id, C.jobs.c.state == cur).values(**vals)).rowcount
                if n:
                    conn.execute(C.job_events.insert().values(job_id=job_id, ts=now, kind="state", from_state=cur, to_state=to, reason=reason, data=redact_obj(event_data or {})))
            if n:
                break
        else:
            raise Conflict(f"job {job_id} is being modified concurrently")
        new = self.get_row(job_id)
        self.ctx.events.publish("job.state_changed", {"job_id": job_id, "from": cur, "to": to, "reason": reason, "workspace_id": row["workspace_id"], "name": row["name"],
                                                      "kind": row["kind"], "attempt": row["attempt"], "item_id": row["item_id"]}, workspace_id=row["workspace_id"], subject=job_id)
        self._notify(job_id)
        if to in TERMINAL:
            self.wake()
        return new

    def _update(self, job_id: str, **vals: Any) -> None:
        vals["updated_at"] = now_iso()
        with self.db.begin() as conn:
            conn.execute(update(C.jobs).where(C.jobs.c.id == job_id).values(**vals))

    def _handle_update(self, job_id: str, **native: Any) -> dict[str, Any]:
        """Merge keys into ``jobs.provider_handle`` atomically enough for our single-writer-per-job use."""
        row = self.get_row(job_id)
        h = dict(row.get("provider_handle") or {})
        h.update(native)
        self._update(job_id, provider_handle=h)
        return h

    # =================================================================== submit
    def submit(self, principal: Principal, sub: JobSubmission, *, internal: bool = False, correlation_id: str | None = None) -> JobRecord:
        """Validate, persist (accepted -> validating -> queued) and return the job. See CONTRACTS 4.1."""
        ws = {"id": SYSTEM_WORKSPACE, "name": "system"} if internal and sub.workspace_id == SYSTEM_WORKSPACE else self._workspace(sub.workspace_id)
        principal.require("job.submit", workspace_id=ws["id"])
        if sub.parent_run_id and not internal:  # lineage may not point into another workspace (a forged parent would show up in that tenant's run tree)
            with self.db.connect() as conn:
                parent = fetch_one(conn, select(C.jobs.c.workspace_id).where(C.jobs.c.id == sub.parent_run_id))
            if parent is not None and parent["workspace_id"] != ws["id"]:
                raise NotFound(f"parent run {sub.parent_run_id} not found")
        request_hash = hashlib.sha256(json.dumps(sub.model_dump(mode="json"), sort_keys=True, default=str).encode()).hexdigest()
        if sub.idempotency_key:
            existing = self._by_idempotency(principal.id, sub.idempotency_key)
            if existing:
                if existing["request_hash"] != request_hash:
                    raise Conflict("idempotency_key was already used with a different job request")
                return self.to_record(existing)
        prep = self._prepare(principal, sub, ws)
        job_id = new_id("job")
        spec_doc, source_rel = self._persist_source(job_id, prep, sub)
        spec_doc["credential"] = principal.credential_ceiling(ws["id"])  # F01: the permission ceiling of the submitting credential; bounds every credential derived for this job
        now = now_iso()
        row = {
            "id": job_id, "kind": sub.kind.value, "engine": prep["engine"], "state": "accepted", "state_reason": "accepted", "workspace_id": ws["id"], "name": prep["name"],
            "item_id": prep["item"]["id"] if prep["item"] else sub.item_id, "submitted_by": principal.id, "origin_kind": sub.origin.kind.value, "origin": sub.origin.details,
            "queue": prep["queue"]["name"], "priority": prep["priority"], "attempt": 1, "max_attempts": 1 + int(prep["policy"].get("retries", 0)),
            "runtime_id": prep["runtime"].runtime_id if prep["runtime"] else None, "runtime_fingerprint": prep["runtime"].fingerprint if prep["runtime"] else None,
            "source_hash": prep["source"]["hash"], "source_path": source_rel, "provider": prep["provider"], "resources_request": prep["resources"], "resources_granted": {},
            "parameters": sub.parameters, "result": {}, "parent_run_id": sub.parent_run_id,
            "correlation_id": correlation_id or principal.correlation_id, "idempotency_key": sub.idempotency_key, "request_hash": request_hash, "spec": spec_doc,
            "created_at": now, "updated_at": now, "timed_out": False,
        }
        try:
            with self.db.begin() as conn:
                insert_row(conn, C.jobs, row)
                conn.execute(C.job_events.insert().values(job_id=job_id, ts=now, kind="state", from_state=None, to_state="accepted", reason="accepted", data={}))
        except IntegrityError:
            shutil.rmtree(self.jobs_dir / job_id, ignore_errors=True)
            existing = self._by_idempotency(principal.id, sub.idempotency_key or "")
            if existing and existing["request_hash"] == request_hash:
                return self.to_record(existing)
            raise Conflict("idempotency_key was already used with a different job request") from None
        self.ctx.events.publish("job.state_changed", {"job_id": job_id, "from": None, "to": "accepted", "reason": "accepted", "workspace_id": ws["id"], "name": row["name"], "kind": row["kind"], "attempt": 1, "item_id": row["item_id"]},
                                workspace_id=ws["id"], subject=job_id)
        self.ctx.audit.record(principal, "job.submitted", resource_type="job", resource_id=job_id, workspace_id=ws["id"],
                              details={"kind": row["kind"], "name": row["name"], "queue": row["queue"], "origin": row["origin_kind"], "item_id": row["item_id"], "runtime": row["runtime_id"]})
        self.transition(job_id, "validating", "validating compatibility and policy", expect=("accepted",))
        try:
            compat = self._analyze(principal, sub, prep)
        except CompatibilityBlocked as exc:
            fail = {"category": FailureCategory.COMPATIBILITY.value, "message": exc.message, "remediation": exc.remediation or "see the compatibility report", "details": exc.details}
            self.transition(job_id, "failed", f"blocked by the compatibility analyzer: {exc.message}"[:500], fields={"failure": fail, "failure_category": "compatibility"})
            exc.details = {**exc.details, "job_id": job_id}
            raise
        except OlcfError as exc:
            self.transition(job_id, "failed", f"validation failed: {exc.message}"[:500], fields={"failure": {"category": FailureCategory.INTERNAL.value, "message": exc.message}, "failure_category": "internal"})
            raise
        if compat is not None:
            spec_doc["compatibility"] = compat
        spec_doc["spec_hash"] = hashlib.sha256(json.dumps({k: v for k, v in spec_doc.items() if k != "spec_hash"}, sort_keys=True, default=str).encode()).hexdigest()
        self._update(job_id, spec=spec_doc, queued_at=now_iso())
        self.transition(job_id, "queued", "queued", expect=("validating",), fields={"queued_at": now_iso()})
        if prep["runtime"] is not None and prep["runtime"].fallback:
            fb = prep["runtime"].fallback
            self.add_event(job_id, "runtime_fallback", fb, reason=str(fb.get("reason"))[:500])
        self.wake()
        return self.get(job_id)

    def rebind_host_python(self, trigger: str = "scheduler pass") -> list[str]:
        """Re-evaluate the runtime of *queued* ``engine=python`` jobs against the workers online now (CONTRACTS addendum L4).

        A job is bound to a runtime when it is submitted. If that runtime is installed on no online worker any more (the only node with it went away, or it was never installed
        and the workers that offer the host Python only connected after the submission), the job would wait for ever. When the host-Python fallback is enabled and some online
        worker offers ``olcf-host-python``, such a job is re-bound to it with a ``runtime_fallback`` timeline event (``rebound: true``). Jobs that pinned a runtime id, that
        asked for Spark/Java/Scala versions, that are not queued any more or whose runtime is still available somewhere are left alone. Returns the re-bound job ids.
        """
        if not bool(self._s("compute.host_python_fallback", True)):
            return []
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs.c.id, C.jobs.c.runtime_id, C.jobs.c.workspace_id, C.jobs.c.spec).where(
                C.jobs.c.state == "queued", C.jobs.c.engine == "python", C.jobs.c.runtime_id.is_not(None), C.jobs.c.runtime_id != HOST_RUNTIME_ID).order_by(C.jobs.c.queued_at.asc()).limit(500))
        if not rows:
            return []
        from olcf.core.models import RuntimeRequest

        rebound: list[str] = []
        verdicts: dict[str, Any] = {}  # identical requests resolve identically within one pass
        for row in rows:
            spec = dict(row.get("spec") or {})
            requested = dict(spec.get("runtime_request") or {})
            if requested.get("runtime_id"):
                continue  # the submitter pinned a runtime: never second-guess it
            key = json.dumps([requested, row["workspace_id"], row["runtime_id"]], sort_keys=True, default=str)
            if key not in verdicts:
                try:
                    sel = self.runtimes.resolve(RuntimeRequest.model_validate({**requested, "spark_conf": spec.get("spark_conf") or {}}), workspace_id=row["workspace_id"], engine="python",
                                                online_only=True)
                except OlcfError:
                    sel = None
                verdicts[key] = sel if sel is not None and sel.runtime_id == HOST_RUNTIME_ID and sel.fallback else None
            sel = verdicts[key]
            if sel is None:
                continue
            spec.update(spark_conf={}, rejected_spark_conf=[], environment=sel.environment, dependency_lock=sel.dependency_lock, runtime_notes=list(sel.notes))
            spec["spec_hash"] = hashlib.sha256(json.dumps({k: v for k, v in spec.items() if k != "spec_hash"}, sort_keys=True, default=str).encode()).hexdigest()
            with self.db.begin() as conn:  # compare-and-set: another replica, a claim or a cancel may have won
                n = conn.execute(update(C.jobs).where(C.jobs.c.id == row["id"], C.jobs.c.state == "queued", C.jobs.c.runtime_id == row["runtime_id"]).values(
                    runtime_id=HOST_RUNTIME_ID, runtime_fingerprint=sel.fingerprint, spec=spec, wait_reason=None, updated_at=now_iso())).rowcount
            if not n:
                continue
            fb = {**(sel.fallback or {}), "requested": row["runtime_id"], "rebound": True, "trigger": trigger}
            fb["reason"] = f"runtime '{row['runtime_id']}' is not available on any online worker; re-bound to the worker's own Python ({HOST_RUNTIME_ID}, no Spark) while queued ({trigger})"
            self.add_event(row["id"], "runtime_fallback", fb, reason=fb["reason"][:500])
            self.ctx.audit.record(SYSTEM_PRINCIPAL, "job.runtime_rebound", resource_type="job", resource_id=row["id"], workspace_id=row["workspace_id"],
                                  details={"from": row["runtime_id"], "to": HOST_RUNTIME_ID, "trigger": trigger})
            rebound.append(row["id"])
        if rebound:
            log.info("re-bound %d queued Python job(s) to %s (%s)", len(rebound), HOST_RUNTIME_ID, trigger)
            self.wake()
        return rebound

    def _by_idempotency(self, principal_id: str, key: str) -> dict[str, Any] | None:
        if not key:
            return None
        with self.db.connect() as conn:
            return fetch_one(conn, select(C.jobs).where(C.jobs.c.submitted_by == principal_id, C.jobs.c.idempotency_key == key))

    def _workspace(self, ref: str) -> dict[str, Any]:
        return self.ctx.service("workspaces").resolve(ref)

    # ---- preparation (everything that can be rejected before a job row exists) ----------------------------------
    def _prepare(self, principal: Principal, sub: JobSubmission, ws: dict[str, Any]) -> dict[str, Any]:
        item = self._load_item(principal, sub, ws)
        settings = (item or {}).get("_settings") or {}
        fields = sub.model_fields_set

        engine = sub.engine if "engine" in fields else (settings.get("engine") or sub.engine)
        resources = sub.resources if "resources" in fields else sub.resources.model_validate({**sub.resources.model_dump(), **(settings.get("resources") or {})})
        # nobody sized this job: the planner scales the defaults to the machine (small laptops just work) instead of enforcing the generic 1 core / 1 GiB shape
        resources_defaulted = "resources" not in fields and not settings.get("resources")
        policy = sub.policy.model_dump()
        if "policy" not in fields:
            policy.update({k: v for k, v in (settings.get("policy") or {}).items() if k in sub.policy.model_fields})
        if policy.get("timeout_seconds") is None and settings.get("timeout_seconds"):
            policy["timeout_seconds"] = int(settings["timeout_seconds"])
        runtime_req = sub.runtime if "runtime" in fields else sub.runtime.model_validate({**sub.runtime.model_dump(), **(settings.get("runtime") or {}),
                                                                                         **({"environment_id": settings["environment_id"]} if settings.get("environment_id") else {})})
        lake_ref = sub.default_lakehouse or (LakehouseRef.model_validate(settings["default_lakehouse"]) if isinstance(settings.get("default_lakehouse"), dict) else None)
        source = self._resolve_source(principal, sub, ws, item)
        if source["format"] not in KIND_FORMATS[sub.kind]:
            raise BadRequest(f"a '{sub.kind.value}' job cannot run a '{source['format']}' source", details={"allowed": sorted(KIND_FORMATS[sub.kind])})
        # queue / pool / provider ---------------------------------------------------------------------------
        qname = policy.get("queue") or "default"
        try:
            queue = self.registry.get_queue(qname)
        except NotFound:
            raise BadRequest(f"queue '{qname}' does not exist", remediation="list queues with GET /v1/queues") from None
        if queue.get("max_queued"):
            with self.db.connect() as conn:
                depth = conn.execute(select(func.count()).select_from(C.jobs).where(C.jobs.c.queue == qname, C.jobs.c.state.in_(("accepted", "validating", "queued")))).scalar() or 0
            if depth >= int(queue["max_queued"]):
                raise TooManyRequests(f"queue '{qname}' is full ({depth} waiting)", remediation="retry later or use another queue")
        placement = dict(policy.get("placement") or {})
        provider_name = placement.get("provider")
        provider_obj = None
        if provider_name:
            try:
                provider_obj = self.providers.get(provider_name)
            except NotFound:
                raise BadRequest(f"provider '{provider_name}' does not exist or is disabled") from None
        elif str(self._s("compute.default_provider", "auto")) not in ("auto", ""):
            provider_name = str(self._s("compute.default_provider", "auto"))
            try:
                provider_obj = self.providers.get(provider_name)
            except NotFound:
                provider_name = None
        pool = placement.get("pool")
        if pool:
            prow = self.registry.get_pool(pool)
            if prow["reserved"] and prow["workspace_ids"] and ws["id"] not in prow["workspace_ids"]:
                raise Forbidden(f"pool '{pool}' is reserved for other workspaces")
        # runtime ---------------------------------------------------------------------------------------------
        uses_runtime = provider_obj is None or getattr(provider_obj, "uses_runtimes", True)
        runtime: RuntimeSelection | None = self.runtimes.resolve(runtime_req, workspace_id=ws["id"], engine=engine) if uses_runtime else None
        # a provider without OLCF runtimes (Livy, Fabric) has no manifest allow-list, but the user's Spark settings are still validated and carried (review F12):
        # platform-owned keys (driver/network/master/...) are refused, everything else under spark.* is the target system's to accept or reject
        remote_conf: tuple[dict[str, str], list[dict[str, str]]] = filter_spark_conf(dict(runtime_req.spark_conf), ["spark.*"]) if runtime is None else ({}, [])
        # lakehouses -----------------------------------------------------------------------------------------
        lakes = self._resolve_lakehouses(principal, sub, ws, lake_ref)
        # secrets -------------------------------------------------------------------------------------------
        for env_name, sid in sub.secret_refs.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", env_name):
                raise BadRequest(f"invalid secret environment variable name '{env_name}'")
            principal.require("secret.use", workspace_id=ws["id"])
            try:
                meta = self.ctx.secrets.get_meta(sid)
            except OlcfError:
                raise NotFound(f"secret '{sid}' not found") from None
            if meta.get("scope_type") == "workspace" and meta.get("scope_id") != ws["id"]:
                raise Forbidden(f"secret '{sid}' belongs to another workspace")
            if meta.get("scope_type") not in ("org", "workspace", None, ""):
                raise Forbidden(f"secret '{sid}' cannot be used by jobs")
        # environment variables: those of the resolved environment first, the submission's own win
        env: dict[str, str] = {str(k): str(v) for k, v in ((runtime.environment or {}).get("variables") or {}).items()} if runtime else {}
        env.update(sub.env)
        for k in env:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", k) or k.startswith("OLCF_"):
                raise BadRequest(f"invalid or reserved environment variable name '{k}'")
        self.check_pip_install_allowed(policy)
        # feasibility (fail closed instead of queueing something that can never run) ---------------------------------
        if provider_obj is None or provider_obj.execution == "agent":
            self._check_feasible(resources, engine, ws["id"], qname, placement, provider_name, runtime, flexible=resources_defaulted and engine == "spark")
        name = sub.name or (item or {}).get("name") or f"{sub.kind.value}-{time.strftime('%Y%m%d-%H%M%S')}"
        return {"item": item, "engine": engine, "resources": resources.model_dump(), "policy": policy, "runtime_req": runtime_req, "runtime": runtime, "source": source,
                "queue": queue, "priority": int(policy.get("priority", queue.get("default_priority", 50))), "provider": provider_name, "lakes": lakes, "name": str(name)[:255],
                "resources_defaulted": resources_defaulted, "env": env, "remote_conf": remote_conf}

    def check_pip_install_allowed(self, policy: dict[str, Any]) -> None:
        """``policy.allow_pip_install`` is honoured only while the administrator's ``environments.pip_policy_max`` is ``allow`` (fail closed otherwise)."""
        if not policy.get("allow_pip_install"):
            return
        settings = self.ctx.maybe_service("settings")
        cap = None
        if settings is not None:
            try:
                cap = settings.get("environments.pip_policy_max")
            except Exception:  # noqa: BLE001 - key not registered (notebooks module absent): no administrator cap exists
                cap = None
        if cap not in (None, "allow"):
            raise PolicyDenied("installing packages from a notebook or job (policy.allow_pip_install) is disabled by the administrator",
                               remediation="declare the packages in an Environment, or ask an administrator to raise environments.pip_policy_max to 'allow'")

    def _check_feasible(self, res: Any, engine: str, ws_id: str, queue: str, placement: dict[str, Any], provider: str | None, runtime: RuntimeSelection | None,
                        flexible: bool = False) -> None:
        views = self.registry.node_views({}, {})
        # the default provider is ``auto`` (standalone when the machine can host it, else local): only an *explicit* standalone request can be infeasible
        # because of its executors, and a request that was not sized by the submitter (``flexible``) is scaled to the machine instead of being refused
        cfg = PlannerConfig(default_provider=str(self._s("compute.default_provider", "auto") or "auto"), driver_reserve_fraction=float(self._s("compute.driver_reserve_fraction", 0.25)))
        jv = JobView(id="probe", workspace_id=ws_id, queue=queue, runtime_id=runtime.runtime_id if runtime else None, engine=engine, driver_cores=res.driver_cores,
                     driver_memory_mb=res.driver_memory_mb, executor_cores=res.executor_cores, executor_memory_mb=res.executor_memory_mb, min_executors=res.min_executors,
                     max_executors=res.max_executors, max_cores=res.max_cores, dynamic_allocation=res.dynamic_allocation, pool=placement.get("pool"),
                     labels=placement.get("labels") or {}, node=placement.get("node"), provider=provider, flexible=flexible)
        why = infeasible_reason(jv, views, cfg)
        if why:
            raise ResourceUnsatisfiable(why, remediation="reduce the request, change the placement constraints or enrol a larger node")

    def _load_item(self, principal: Principal, sub: JobSubmission, ws: dict[str, Any]) -> dict[str, Any] | None:
        if not sub.item_id:
            return None
        items = self.ctx.service("items")
        item = items.get(sub.item_id)
        if item["workspace_id"] != ws["id"]:
            raise NotFound(f"item {sub.item_id} does not belong to workspace {ws['id']}")  # never reveal cross-workspace existence details
        principal.require("item.read", workspace_id=ws["id"], item_id=item["id"])
        d = dict(item.get("definition") or {})
        item = dict(item)
        item["_settings"] = d.get("settings") or {}
        return item

    def _resolve_source(self, principal: Principal, sub: JobSubmission, ws: dict[str, Any], item: dict[str, Any] | None) -> dict[str, Any]:
        src: JobSource | None = sub.source
        fmt, content, uri, filename, main_class, args = None, None, None, None, None, []
        if src is not None and (src.content is not None or src.artifact_uri):
            fmt, content, uri, filename = src.format, src.content, src.artifact_uri, src.filename
            main_class, args = src.main_class, [str(a) for a in src.args]
        elif item is not None:
            d = item.get("definition") or {}
            typ = item["type"]
            if typ == "notebook":
                fmt = "ipynb"
                nbsvc = self.ctx.maybe_service("notebooks")
                if nbsvc is not None:
                    nb = nbsvc.load(item)
                    content = nbsvc.to_ipynb(nb)
                    content = content if isinstance(content, str) else json.dumps(content)
                else:
                    content = json.dumps(d.get("nbformat") or {})
            elif typ == "spark_job_definition":
                lang = str(d.get("language") or "python").lower()
                main_class, args = d.get("main_class"), [str(a) for a in (d.get("args") or [])]
                uri = d.get("main_file") or d.get("artifact_uri")
                content = d.get("code")
                fmt = "jar" if (main_class or str(uri or "").endswith(".jar")) else ("scala" if lang == "scala" else "py")
            elif typ == "sql_query":
                fmt, content = "sql", str(d.get("sql") or "")
            else:
                raise BadRequest(f"items of type '{typ}' cannot be run as a job")
            filename = item["name"]
        else:
            raise BadRequest("a job needs either an item_id or an inline source", remediation="set 'item_id' or 'source': {format, content}")
        if content is None and not uri:
            raise BadRequest("the job source is empty")
        data: bytes | None = None
        if content is not None:
            data = content.encode("utf-8")
            if src is not None and src.sha256 and src.sha256.replace("sha256:", "") != hashlib.sha256(data).hexdigest():
                raise ValidationFailed("source.sha256 does not match the supplied content")
        if data is None and uri:
            remote = self.providers_remote_for(sub)
            if not remote:
                data = self._load_artifact(principal, uri, ws["id"], sub)
        if data is not None and len(data) > MAX_INLINE_SOURCE:
            raise Unsupported(f"the job source is {len(data) // 1024 // 1024} MiB; the limit is {MAX_INLINE_SOURCE // 1024 // 1024} MiB", remediation="stage large artifacts as runtime jars/packages")
        if data is None and not uri:
            raise BadRequest("the job source is empty")
        return {"format": fmt or "script", "bytes": data, "artifact_uri": uri, "filename": filename, "hash": _digest(data) if data is not None else None,
                "main_class": main_class, "args": args}

    def providers_remote_for(self, sub: JobSubmission) -> bool:
        name = (sub.policy.placement or {}).get("provider")
        if not name:
            return False
        try:
            return self.providers.get(name).execution == "remote"
        except OlcfError:
            return False

    def _load_artifact(self, principal: Principal, uri: str, ws_id: str, sub: JobSubmission) -> bytes:
        st = self.ctx.maybe_service("storage")
        if st is None:
            raise Unavailable("the storage service is required to load artifact_uri sources")
        default_lh = sub.default_lakehouse.lakehouse_id if sub.default_lakehouse and sub.default_lakehouse.lakehouse_id else None
        loc = st.resolve(uri, default_lh, workspace_id=ws_id)
        lh = st.get_lakehouse(loc.lakehouse_id)
        principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
        data = st.read_bytes(lh, f"{loc.area}/{loc.relative_path}".strip("/"))
        return bytes(data)

    def _resolve_lakehouses(self, principal: Principal, sub: JobSubmission, ws: dict[str, Any], default_ref: LakehouseRef | None) -> dict[str, Any]:
        refs: list[tuple[LakehouseRef, str]] = []
        if default_ref is not None:
            refs.append((default_ref, "default"))
        for d in sub.data_refs:
            if d.kind == "lakehouse":
                refs.append((LakehouseRef(lakehouse_id=d.ref) if d.ref.startswith("itm_") else LakehouseRef(name=d.ref), d.role))
        if not refs:
            return {"default": None, "all": [], "ids": []}
        st = self.ctx.maybe_service("storage")
        if st is None:
            raise Unavailable("the storage service is not available, so lakehouses cannot be attached")
        out: list[dict[str, Any]] = []
        ids: list[str] = []
        default_desc = None
        for ref, role in refs:
            lh = self._find_lakehouse(st, ref, ws["id"])
            principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
            if role == "write":
                principal.require("lakehouse.write", workspace_id=lh.workspace_id, item_id=lh.id)
            desc = st.job_lakehouse_descriptor(lh)
            if role == "default":
                default_desc = desc
            if lh.id not in ids:
                ids.append(lh.id)
                out.append(desc)
        return {"default": default_desc, "all": [d for d in out if not default_desc or d["id"] != default_desc["id"]], "ids": ids}

    def _find_lakehouse(self, st: Any, ref: LakehouseRef, ws_id: str) -> Any:
        try:
            if ref.lakehouse_id:
                return st.get_lakehouse(ref.lakehouse_id)
            if ref.name:
                return st.get_lakehouse(ref.name, workspace_id=ref.workspace_id or ws_id)
            if ref.fabric_lakehouse_id:
                items = self.ctx.service("items")
                for it in items.list(ref.workspace_id or ws_id, type_="lakehouse"):
                    fab = (it.get("definition") or {}).get("fabric") or {}
                    if fab.get("lakehouse_id") == ref.fabric_lakehouse_id and (not ref.fabric_workspace_id or fab.get("workspace_id") == ref.fabric_workspace_id):
                        return st.get_lakehouse(it["id"])
        except NotFound:
            pass
        raise NotFound("the referenced lakehouse was not found", details={"ref": ref.model_dump(exclude_none=True)})

    def _persist_source(self, job_id: str, prep: dict[str, Any], sub: JobSubmission) -> tuple[dict[str, Any], str | None]:
        src = prep["source"]
        rel = None
        if src["bytes"] is not None:
            rel = f"source/{SOURCE_NAMES.get(src['format'], 'source.txt')}"
            d = self.jobs_dir / job_id / "source"
            d.mkdir(parents=True, exist_ok=True)
            p = d / SOURCE_NAMES.get(src["format"], "source.txt")
            fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(src["bytes"])
        sub_doc = {"queue": prep["queue"]["name"], "name": prep["name"], "env": dict(prep["env"]), "secret_refs": dict(sub.secret_refs), "tags": dict(sub.tags),
                   "resources_defaulted": bool(prep["resources_defaulted"])}
        rt, lakes = prep["runtime"], prep["lakes"]
        spec = {
            "source": {"format": src["format"], "path": rel, "sha256": (src["hash"] or "").removeprefix("sha256:") or None, "artifact_uri": src["artifact_uri"], "main_class": src["main_class"], "args": src["args"],
                       "filename": src["filename"], "size": len(src["bytes"]) if src["bytes"] is not None else None},
            "policy": prep["policy"], "runtime_request": prep["runtime_req"].model_dump(exclude={"spark_conf"}),
            "spark_conf": dict(rt.spark_conf) if rt else dict(prep["remote_conf"][0]), "rejected_spark_conf": list(rt.rejected_conf) if rt else list(prep["remote_conf"][1]),
            "environment": rt.environment if rt else None, "dependency_lock": rt.dependency_lock if rt else None,
            "default_lakehouse": lakes["default"], "lakehouses": lakes["all"], "lakehouse_ids": lakes["ids"],
            "runtime_notes": list(rt.notes) if rt else [], **sub_doc,
        }
        return spec, rel

    # ---- compatibility ------------------------------------------------------------------------------------------
    def _analyze(self, principal: Principal, sub: JobSubmission, prep: dict[str, Any]) -> dict[str, Any] | None:
        """Run the FABRIC analyzer when the origin or source is Fabric-shaped. Blockers fail closed with ``CompatibilityBlocked``."""
        src = prep["source"]
        text = src["bytes"].decode("utf-8", "replace") if src["bytes"] is not None else ""
        fabric_origin = sub.origin.kind == OriginKind.FABRIC or src["format"] == "fabric_git_source"
        looks_fabric = bool(text) and src["format"] in ("ipynb", "py") and bool(FABRIC_MARKERS.search(text))
        pre = sub.compatibility if isinstance(sub.compatibility, dict) else None
        report: dict[str, Any] | None = pre
        if fabric_origin or looks_fabric:
            svc = self.ctx.maybe_service("fabric")
            if svc is not None and text:
                try:
                    res = svc.analyze(text, format=src["format"], runtime_family=prep["runtime"].family if prep["runtime"] else None, origin=sub.origin.model_dump(mode="json"))
                except OlcfError:
                    raise
                except Exception as exc:  # noqa: BLE001 - fail closed: an analyzer that cannot answer must not let the job through
                    raise Unavailable(f"the compatibility analyzer failed: {exc}", remediation="retry, or ask an administrator to check the Fabric adapter") from exc
                report = self._as_report(res)
        if report and (report.get("blocked") or report.get("blockers")):
            blockers = report.get("blockers") or []
            raise CompatibilityBlocked("the notebook uses constructs that OLCF cannot run" + (f": {', '.join(str(b.get('rule', b))[:60] for b in blockers[:5])}" if blockers else ""),
                                       details={"report": redact_obj(report)}, remediation="see the compatibility report; replace or remove the blocking constructs")
        return redact_obj(report) if report else None

    @staticmethod
    def _as_report(res: Any) -> dict[str, Any]:
        if isinstance(res, dict):
            return res
        if hasattr(res, "model_dump"):
            return res.model_dump(mode="json")
        return {"blocked": bool(getattr(res, "blocked", False)), "blockers": list(getattr(res, "blockers", []) or []), "summary": getattr(res, "summary", None)}

    # =================================================================== scheduler interface
    def claim(self, job_id: str, placement: Any, *, queue_wait: float | None = None) -> dict[str, Any] | None:
        """queued -> provisioning with the planner's decision recorded. ``None`` if the job is no longer queued (cancelled meanwhile)."""
        granted = dict(placement.granted())
        granted["provider"] = placement.provider
        if placement.cluster_key:  # lets the scheduler account executor usage per cluster before the master reports the application
            granted["cluster_runtime"], granted["cluster_pool"] = placement.cluster_key
        why = f"placed on {placement.provider}" + (f" node {placement.node_id}" if placement.node_id else "") + (f" ({placement.note})" if getattr(placement, "note", None) else "")
        fields: dict[str, Any] = {"provider": placement.provider, "driver_worker_id": placement.node_id, "resources_granted": granted, "wait_reason": None, "not_before": None,
                                  "lost_since": None, "provider_handle": None}
        if placement.runtime_id == HOST_RUNTIME_ID and placement.node_id:
            # the host-Python runtime is the interpreter of whichever node runs the job: record that node's fingerprint (Python, platform, library versions)
            fp = self.runtimes.node_fingerprint(HOST_RUNTIME_ID, placement.node_id)
            if fp:
                fields["runtime_fingerprint"] = fp
        row = self.transition(job_id, "provisioning", why[:500], expect=("queued",), fields=fields,
                              event_data={"queue_wait_seconds": queue_wait, "node": placement.node_id, "provider": placement.provider, "note": getattr(placement, "note", None)})
        if row is None:
            return None
        now = now_iso()
        try:
            with self.db.begin() as conn:
                insert_row(conn, C.job_attempts, {"id": new_id("att"), "job_id": job_id, "attempt": row["attempt"], "worker_id": placement.node_id, "cluster_id": None,
                                                  "provider": placement.provider, "state": "provisioning", "created_at": now})
        except IntegrityError:
            pass  # the attempt row exists (claim re-run after a crash)
        self._metric("olcf_job_queue_wait_seconds", queue_wait, {"queue": row["queue"]})
        return row

    def _metric(self, name: str, value: float | None, labels: dict[str, str]) -> None:
        if value is None:
            return
        svc = self.ctx.maybe_service("metrics")
        if svc is None:
            return
        try:
            svc.record_sample(name, float(value), labels)
        except Exception:  # noqa: BLE001
            log.debug("metric %s not recorded", name, exc_info=True)

    def launch_async(self, job_id: str) -> None:
        self._pool.submit(self.launch, job_id)

    def launch(self, job_id: str) -> None:
        """Provider hand-off for a provisioning job (runs in the launch pool). Failures are classified and retried or failed here."""
        with self._waiters_lock:
            if job_id in self._launching:
                return
            self._launching.add(job_id)
        try:
            row = self.get_row(job_id)
            if row["state"] != "provisioning":
                return
            provider = self.providers.get(row["provider"])
            spec_doc = row["spec"] or {}
            granted = row["resources_granted"] or {}
            runtime_row = self.runtimes.get_row(row["runtime_id"]) if row["runtime_id"] else {"manifest": {}}
            node: NodeInfo | None = NodeInfo.from_row(self.registry.get_worker(row["driver_worker_id"])) if row["driver_worker_id"] else None
            if provider.execution == "agent" and (node is None or not self.hub.is_connected(node.id)):
                # connected to another replica is fine (outbox); only a node that is offline everywhere is a problem
                if node is None or node.state not in ("online", "degraded"):
                    raise WorkerOffline(f"driver node {row['driver_worker_id']} is not online")
            spark_block = None
            if row["engine"] == "spark" and getattr(provider, "prepares_spark", True):
                spark_block = provider.prepare(PrepareRequest(job=row, placement=granted, runtime=runtime_row, spark_conf=dict(spec_doc.get("spark_conf") or {}),
                                                              app_name=f"olcf-{job_id}", node=node))
            elif not getattr(provider, "prepares_spark", True):
                # provider-neutral Spark section for targets that build their own session (Livy): the validated user conf travels in the spec and the provider
                # translates / filters it (review F12); cluster preparation (master, driver host and ports) stays agent-only
                spark_block = {"master": None, "app_name": f"olcf-{job_id}", "conf": dict(spec_doc.get("spark_conf") or {}), "jars": [], "packages": []}
            source_bytes = self._read_source(row)
            token, token_id = self._mint_job_token(row)
            try:
                # record the id BEFORE anything else can fail (secret resolution in _build_spec, provider.submit ...): a token whose id is not on the attempt row cannot be listed
                # or revoked by _end_attempt / _launch_failed and would stay usable until its TTL
                self._record_attempt_token(job_id, row["attempt"], token_id)
            except Exception:
                self._revoke_token(token_id)
                raise
            spec = self._build_spec(row, spark_block, token)
            cluster_id = (spark_block or {}).get("cluster_id")
            req = SubmitRequest(job=row, spec=spec, source_bytes=source_bytes, source_path=row["source_path"], worker_id=row["driver_worker_id"], cluster_id=cluster_id,
                                timeout=float(self._s("compute.dispatch_timeout_seconds", 60)))
            intent = provider.submission_intent(req) if provider.execution == "remote" else None
            if intent:
                # persist the intent *before* the create request leaves: a crash in between is then reconciled against the remote system (recover()), never replayed
                self._update(job_id, provider_handle={**intent, "sent": False, "intent_at": now_iso()})
            handle = provider.submit(req)
            native = {**handle.native, "sent": True, "sent_at": now_iso(), "log_seq": 0, "log_offset": 0}
            with self.db.begin() as conn:
                # the agent may already have reported 'running' (or the user cancelled) before run_job's result came back: record the hand-off in every live state
                conn.execute(update(C.jobs).where(C.jobs.c.id == job_id, C.jobs.c.state.in_(("provisioning", "running", "cancel_requested"))).values(
                    provider_handle=native, cluster_id=cluster_id or handle.cluster_id, application_id=handle.application_id or None, updated_at=now_iso()))
                conn.execute(update(C.job_attempts).where(C.job_attempts.c.job_id == job_id, C.job_attempts.c.attempt == row["attempt"]).values(cluster_id=cluster_id, token_id=token_id))
            self.add_event(job_id, "dispatched", {"provider": row["provider"], "worker_id": row["driver_worker_id"], "cluster_id": cluster_id})
            after = self.get_row(job_id)
            if after["state"] in TERMINAL or after["state"] == "cancel_requested":
                # cancelled while run_job was in flight: the cancel path could not know the process existed, so stop it now
                log.info("job %s was cancelled while it was being started; stopping the process", job_id)
                try:
                    provider.cancel(handle, force=True, grace_seconds=0)
                except OlcfError as exc:
                    log.warning("stopping %s after a racing cancel failed: %s", job_id, exc.message)
        except Exception as exc:  # noqa: BLE001
            self._launch_failed(job_id, exc)
        finally:
            with self._waiters_lock:
                self._launching.discard(job_id)

    def _read_source(self, row: dict[str, Any]) -> bytes | None:
        if not row.get("source_path"):
            return None
        p = (self.jobs_dir / row["id"] / row["source_path"]).resolve()
        if (self.jobs_dir / row["id"]).resolve() not in p.parents:
            raise BadRequest("invalid source path")
        return p.read_bytes() if p.is_file() else None

    def _mint_job_token(self, row: dict[str, Any]) -> tuple[str, str]:
        timeout = int((row["spec"] or {}).get("limits", {}).get("timeout_seconds") or (row["spec"] or {}).get("policy", {}).get("timeout_seconds")
                      or self.ctx.settings.compute.default_job_timeout_seconds)
        pid = row["submitted_by"] or SYSTEM_PRINCIPAL.id
        # F01: the attempt credential is the intersection of the ceiling persisted at submission, the owner's CURRENT role bindings and the minimum ``job`` scope, bound to
        # this job's workspace; ``olcf.api.deps.authenticate`` enforces it on every request. Secrets the job references are re-authorised here, just in time.
        cred = (row["spec"] or {}).get("credential")
        if not isinstance(cred, dict) or cred.get("workspace_id") != row["workspace_id"]:  # submitted before ceilings were persisted: fail closed to read-only
            log.warning("job %s carries no credential ceiling; its callback credential is read-only", row["id"])
            cred = fixed_ceiling(LEGACY_JOB_PERMISSIONS, row["workspace_id"])
        if self._submission_secret_refs(row):
            owner = self.ctx.service("identity").get(pid)
            if owner is None or not owner.get("is_active", True):
                raise Forbidden(f"the principal that submitted job {row['id']} no longer exists or is disabled")
            self.ctx.authz.principal_from_row(owner, token_scopes=["job"], credential=cred).require("secret.use", workspace_id=row["workspace_id"])
        plaintext, rec = self.ctx.tokens.create_derived(pid, cred, name=f"job {row['id']}", ttl=timeout + 3600, binding={"job_id": row["id"], "attempt": row["attempt"]})
        return plaintext, rec["id"]

    def _record_attempt_token(self, job_id: str, attempt: int, token_id: str) -> None:
        with self.db.begin() as conn:
            conn.execute(update(C.job_attempts).where(C.job_attempts.c.job_id == job_id, C.job_attempts.c.attempt == attempt).values(token_id=token_id))

    def _revoke_token(self, token_id: str) -> None:
        try:
            self.ctx.tokens.revoke(token_id)
        except Exception:  # noqa: BLE001
            log.warning("could not revoke job token %s", token_id, exc_info=True)

    def _revoke_job_tokens(self, job_id: str) -> None:
        with self.db.connect() as conn:
            ids = [r["token_id"] for r in fetch_all(conn, select(C.job_attempts.c.token_id).where(C.job_attempts.c.job_id == job_id)) if r["token_id"]]
        for tid in ids:
            try:
                self.ctx.tokens.revoke(tid)
            except Exception:  # noqa: BLE001
                log.debug("token revoke failed", exc_info=True)

    def _build_spec(self, row: dict[str, Any], spark_block: dict[str, Any] | None, job_token: str) -> dict[str, Any]:
        """The ``job.json`` of CONTRACTS 4.3 including the sensitive parts (resolved just-in-time; never persisted)."""
        d = row["spec"] or {}
        ws = self._ws_name(row["workspace_id"])
        policy = d.get("policy") or {}
        timeout = int(policy.get("timeout_seconds") or self.ctx.settings.compute.default_job_timeout_seconds)
        secrets: dict[str, str] = {}
        for env_name, sid in (self._submission_secret_refs(row)).items():
            val = self.ctx.secrets.resolve(sid)
            secrets[env_name] = val if isinstance(val, str) else json.dumps(val)
        storage_conf: dict[str, Any] = {"spark": {}, "env": {}}
        st = self.ctx.maybe_service("storage")
        if st is not None:
            for lid in d.get("lakehouse_ids") or []:
                conf = st.job_storage_conf(lid)
                storage_conf["spark"].update(conf.get("spark") or {})
                storage_conf["env"].update(conf.get("env") or {})
        rt = None
        if row["runtime_id"]:
            m = self.runtimes.get_row(row["runtime_id"])
            rt = {"id": row["runtime_id"], "family": m["family"], "fingerprint": row["runtime_fingerprint"], "dependency_lock": d.get("dependency_lock"), "environment": d.get("environment")}
        origin = {"kind": row["origin_kind"], **(row["origin"] or {})}
        lake = d.get("default_lakehouse")
        spec: dict[str, Any] = {
            "schema": "olcf.jobspec/1", "job_id": row["id"], "attempt": row["attempt"], "kind": row["kind"], "engine": row["engine"],
            "workspace": {"id": row["workspace_id"], "name": ws}, "name": row["name"],
            "source": {"format": (d.get("source") or {}).get("format"), "path": row["source_path"], "sha256": (row["source_hash"] or "").removeprefix("sha256:") or None, "main_class": (d.get("source") or {}).get("main_class"),
                       "args": (d.get("source") or {}).get("args") or [], "artifact_uri": (d.get("source") or {}).get("artifact_uri")},
            "parameters": row["parameters"] or {}, "spark": spark_block, "runtime": rt, "default_lakehouse": lake, "lakehouses": d.get("lakehouses") or [],
            # the node honours ``policy.allow_pip_install`` (CONTRACTS N3): %pip of packages that are not in the declared environment
            "policy": {"allow_pip_install": bool(policy.get("allow_pip_install")), "idempotent": bool(policy.get("idempotent"))},
            "storage_conf": storage_conf, "env": dict(d.get("env") or {}), "secrets": secrets,
            "limits": {"timeout_seconds": timeout, "max_output_bytes": int((d.get("limits") or {}).get("max_output_bytes") or 50 * 1024 * 1024), "max_rows": 1000},
            "callbacks": {"api_url": self.api_url(), "job_token": job_token},
            "context": {"workspace_id": row["workspace_id"], "workspace_name": ws, "item_id": row["item_id"], "job_id": row["id"], "run_id": row["id"], "attempt": row["attempt"],
                        "origin": origin, "submitted_by": row["submitted_by"], "parent_run_id": row["parent_run_id"], "queue": row["queue"],
                        "default_lakehouse_id": (lake or {}).get("id"), "fabric": (origin.get("fabric") or (lake or {}).get("fabric")), "tags": d.get("tags") or {}},
        }
        if d.get("compatibility"):
            spec["capsule"] = {"compatibility": d["compatibility"]}
        return spec

    def _submission_secret_refs(self, row: dict[str, Any]) -> dict[str, str]:
        return dict((row["spec"] or {}).get("secret_refs") or {})

    def _ws_name(self, ws_id: str) -> str:
        if ws_id == SYSTEM_WORKSPACE:
            return "system"
        try:
            return str(self._workspace(ws_id)["name"])
        except OlcfError:
            return ws_id

    # ---- launch failure handling ----------------------------------------------------------------------------------
    def _launch_failed(self, job_id: str, exc: Exception) -> None:
        msg = redact_text(exc.message if isinstance(exc, OlcfError) else f"{type(exc).__name__}: {exc}")
        infra = isinstance(exc, (WorkerOffline, WorkerTimeout, WorkerCommandFailed, ProviderError, Unavailable))
        if not isinstance(exc, OlcfError):
            log.exception("launch of %s failed", job_id)
        failure = {"category": (FailureCategory.PROVIDER if infra else FailureCategory.INTERNAL).value, "message": f"could not start the job: {msg}",
                   "remediation": getattr(exc, "remediation", None) or ("check worker and provider health (GET /v1/compute/topology)" if infra else None)}
        if isinstance(exc, (BadRequest, Unsupported, ValidationFailed, Forbidden, NotFound)):
            failure["category"] = FailureCategory.RESOURCE.value if isinstance(exc, ResourceUnsatisfiable) else FailureCategory.INTERNAL.value
        row = self.get_row(job_id)
        # the attempt credential minted for this launch is useless now (the provider refused or never answered): revoke it at once, before the retry / failure decision,
        # whatever state the job is in - the requeue path below does not end the attempt, so nothing else would
        self._revoke_job_tokens_for_attempt(row)
        if row["state"] != "provisioning":
            return
        if isinstance(exc, WorkerOffline) and row["attempt"] < 5:
            # the chosen worker still looks online in the database (e.g. right after a control-plane restart) but its connection is not up yet: nothing was started,
            # so wait for the next placement instead of failing the job
            self._requeue(row, failure, 3, "the worker is not connected yet, placing the job again", msg)
            return
        self._finish(row, "failed", failure=failure, infra_loss=infra, confirmed_gone=True, reached_running=False, reason=f"launch failed: {msg}")

    # =================================================================== agent callbacks (hub listener threads)
    def _on_job_state(self, worker_id: str, msg: P.JobStateMsg, frame: P.Frame) -> None:  # noqa: ARG002
        try:
            row = self.get_row(msg.job_id)
        except NotFound:
            log.warning("job_state for unknown job %s from %s", msg.job_id, worker_id)
            return
        if row["driver_worker_id"] != worker_id:
            log.warning("ignoring job_state for %s from %s (driver is %s)", msg.job_id, worker_id, row["driver_worker_id"])
            return
        if msg.attempt is not None and msg.attempt != row["attempt"]:
            log.info("ignoring stale job_state for %s attempt %s (current %s)", msg.job_id, msg.attempt, row["attempt"])
            return
        if row["state"] in TERMINAL:
            return  # an agent re-sends terminal states after reconnecting; duplicates are harmless
        if msg.state == "running":
            self._mark_running(row)
            return
        res, pending = artifacts_mod.ingest_inline(self.jobs_dir, row["id"], dict(msg.result or {}))  # stores inline artifacts, strips their base64 from the result
        failure = msg.failure or res.get("failure")
        self._finish(row, msg.state, failure=failure, exit_code=msg.exit_code, result=res, confirmed_gone=True, infra_loss=False)
        if pending:
            self._pool.submit(self._fetch_artifacts, row["id"], worker_id, pending)

    def _fetch_artifacts(self, job_id: str, worker_id: str, pending: list[dict[str, Any]]) -> None:
        """Pull the artifacts that did not fit into the terminal ``job_state`` frame (``fetch_artifact``), one by one; failures are job events, never job failures."""
        cap = int(self._s("compute.max_artifact_bytes", artifacts_mod.HARD_ARTIFACT_CAP))
        for entry in pending:
            try:
                info = artifacts_mod.fetch_remote(self.jobs_dir, job_id, entry, lambda t, p: self.hub.request(worker_id, t, p, timeout=60.0), cap=cap)
                self.add_event(job_id, "artifact_fetched", info)
            except Exception as exc:  # noqa: BLE001
                msg = exc.message if isinstance(exc, OlcfError) else str(exc)
                log.warning("artifact %s of %s could not be fetched from %s: %s", entry.get("name"), job_id, worker_id, msg)
                try:
                    self.add_event(job_id, "artifact_failed", {"name": entry.get("name"), "error": msg[:300]})
                except Exception:  # noqa: BLE001
                    log.debug("could not record artifact_failed", exc_info=True)

    def _mark_running(self, row: dict[str, Any]) -> dict[str, Any] | None:
        now = now_iso()
        timeout = int(((row["spec"] or {}).get("policy") or {}).get("timeout_seconds") or self.ctx.settings.compute.default_job_timeout_seconds)
        fields = {"started_at": row["started_at"] or now, "timeout_at": row["timeout_at"] or iso_in(timeout), "lost_since": None}
        if row["state"] == "provisioning":
            new = self.transition(row["id"], "running", "the application started", expect=("provisioning",), fields=fields)
            with self.db.begin() as conn:
                conn.execute(update(C.job_attempts).where(C.job_attempts.c.job_id == row["id"], C.job_attempts.c.attempt == row["attempt"]).values(state="running", started_at=now))
            return new
        if row["state"] in ("running", "cancel_requested"):
            self._update(row["id"], lost_since=None)
        return None

    def _on_job_event(self, worker_id: str, msg: P.JobEventMsg, frame: P.Frame) -> None:  # noqa: ARG002
        try:
            row = self.get_row(msg.job_id)
        except NotFound:
            return
        if row["driver_worker_id"] != worker_id:
            return
        extra = msg.model_extra or {}
        if extra.get("attempt") is not None and extra["attempt"] != row["attempt"]:
            return  # an event of an earlier attempt
        seq = extra.get("seq")
        if isinstance(seq, int) and not isinstance(seq, bool):
            # agents re-send events after a reconnect; ``seq`` (the raw log offset of the @@OLCF@@ line) is the dedupe id (CONTRACTS, NODE addenda N3)
            with self._event_lock:
                h = dict(self.get_row(row["id"]).get("provider_handle") or {})
                seen = h.get("event_seq") or {}
                if seen.get("attempt") == row["attempt"] and seq <= int(seen.get("seq", -1)):
                    return
                self._handle_update(row["id"], event_seq={"attempt": row["attempt"], "seq": seq})
        self.handle_job_event(row, msg.event, msg.data)

    def handle_job_event(self, row: dict[str, Any], event: str, data: dict[str, Any]) -> None:
        """Shared by agent ``job_event`` frames and the remote-provider log scraper (``@@OLCF@@`` lines).

        ``dataset_read`` / ``dataset_written`` / ``dataset_statement`` (what the job's code read, wrote and ran, see :mod:`olcf.runtime_lib.lineage_probe`) are validated and
        forwarded as the platform events ``dataset.read`` / ``dataset.written`` / ``dataset.statement`` by :meth:`forward_dataset_event`; they never raise.
        """
        job_id = row["id"]
        if event in lineage_events.RUNNER_EVENTS:
            self.forward_dataset_event(row, event, data)
            return
        self.add_event(job_id, event, data)
        if event == "spark_app_started" and data.get("application_id"):
            app_id = str(data["application_id"])
            ui = data.get("ui_url")
            now = now_iso()
            with self.db.begin() as conn:
                conn.execute(update(C.jobs).where(C.jobs.c.id == job_id).values(application_id=app_id, updated_at=now))
                cluster_id = row["cluster_id"] or ""
                cur = fetch_one(conn, select(C.spark_apps).where(C.spark_apps.c.application_id == app_id, C.spark_apps.c.cluster_id == cluster_id))
                vals = {"job_id": job_id, "workspace_id": row["workspace_id"], "name": data.get("app_name") or f"olcf-{job_id}", "driver_worker_id": row["driver_worker_id"],
                        "updated_at": now, "last_seen_at": now}
                if ui:
                    vals["ui_url"] = str(ui)
                if cur is None:
                    insert_row(conn, C.spark_apps, {"id": new_id("sap"), "application_id": app_id, "cluster_id": cluster_id, "state": "RUNNING", "executors": [], "started_at": now,
                                                    "created_at": now, **{"ui_url": None, **vals}})
                else:
                    conn.execute(update(C.spark_apps).where(C.spark_apps.c.id == cur["id"]).values(**vals))
            self.ctx.events.publish("job.spark_app_started", {"job_id": job_id, "application_id": app_id}, workspace_id=row["workspace_id"], subject=job_id)
        elif event == "executor_hosts" and row.get("application_id"):
            hosts = data.get("hosts") or data.get("executors") or []
            with self.db.begin() as conn:
                conn.execute(update(C.spark_apps).where(C.spark_apps.c.job_id == job_id).values(executors=redact_obj(hosts) if isinstance(hosts, list) else [], updated_at=now_iso()))

    # =================================================================== dataset events (runtime lineage)
    def forward_dataset_event(self, row: dict[str, Any], event: str, data: Any) -> bool:
        """Turn a runner ``dataset_*`` event into a platform ``dataset.*`` event with the job's context; ``True`` when one was published.

        The runner is untrusted (it executes user code): the payload is validated and clipped by :func:`olcf.compute.lineage_events.sanitize_runner_event` and limited to
        :data:`olcf.compute.lineage_events.MAX_EVENTS_PER_JOB` events per attempt. A malformed or over-budget event is dropped and counted (``dataset_event_stats``, metric
        ``olcf_dataset_events_dropped_total{reason}``); an event-store failure is swallowed. This method never raises, so no runner line can fail a job.
        ``row`` is a job row or, for an interactive session, ``{"kind": "session", "id", "workspace_id", "item_id", "name", "engine", "spec"}``.
        """
        try:
            spec = row.get("spec") or {}
            default = (spec.get("default_lakehouse") or {}).get("id") if isinstance(spec.get("default_lakehouse"), dict) else None
            allowed = {str(i) for i in (spec.get("lakehouse_ids") or []) if isinstance(i, str)} | ({str(default)} if default else set())
            etype, payload = lineage_events.sanitize_runner_event(event, data, row=row, allowed_lakehouses=allowed, default_lakehouse=str(default) if default else None,
                                                                  fabric_lakehouses=lineage_events.fabric_lakehouses_of(spec))
            if not self.dataset_event_budget.take(f"{row['id']}:{row.get('attempt', 1)}"):
                raise lineage_events.MalformedDatasetEvent("over_budget")
        except lineage_events.MalformedDatasetEvent as exc:
            self._dataset_event_dropped(row, event, exc.reason)
            return False
        except Exception:  # noqa: BLE001 - nothing a runner prints may break event handling
            self._dataset_event_dropped(row, event, "internal_error")
            return False
        ws, subject = row.get("workspace_id"), str(row["id"])
        published = False
        if row.get("kind") != "session":
            with dsev.best_effort(log, "dataset job event"):
                self.add_event(row["id"], event, lineage_events.compact_for_job_log(etype, payload))
        with dsev.best_effort(log, etype):
            if etype == "dataset.read":
                self.ctx.events.publish("dataset.read", payload, workspace_id=ws, subject=subject)
            elif etype == "dataset.written":
                self.ctx.events.publish("dataset.written", payload, workspace_id=ws, subject=subject)
            else:
                # the SQL text stays out of the persisted event table (webhooks / GET /v1/events / SSE read it): only in-process subscribers receive it
                self.ctx.events.publish("dataset.statement", payload, workspace_id=ws, subject=subject, persist=False)
            published = True
        if published:
            self.dataset_event_stats.forward()
        return published

    def _dataset_event_dropped(self, row: dict[str, Any], event: str, reason: str) -> None:
        self.dataset_event_stats.drop(reason)
        log.debug("dropped %s of %s: %s", event, row.get("id"), reason)
        try:
            reg = self.ctx.maybe_service("metrics")
            if reg is not None:
                reg.counter("olcf_dataset_events_dropped_total", "Runner dataset events dropped as malformed or over budget").inc(1.0, reason=reason)
        except Exception:  # noqa: BLE001
            log.debug("dataset event drop counter not recorded", exc_info=True)

    # =================================================================== finishing / retry
    def _finish(self, row: dict[str, Any], outcome: str, *, failure: dict[str, Any] | None = None, exit_code: int | None = None, result: dict[str, Any] | None = None,
                infra_loss: bool = False, confirmed_gone: bool = True, reached_running: bool | None = None, reason: str | None = None) -> dict[str, Any] | None:
        """Central terminal handling for one attempt (success, failure with retry decision, cancellation, timeout)."""
        job_id = row["id"]
        cur = self.get_row(job_id)
        if cur["state"] in TERMINAL:
            return None
        row = cur
        result = result or {}
        if reached_running is None:
            reached_running = row["started_at"] is not None
        if row["timed_out"] and outcome in ("failed", "cancelled", "timed_out"):
            outcome = "timed_out"
        elif row["state"] == "cancel_requested" and outcome == "failed":
            outcome = "cancelled"
        if outcome == "succeeded" and row["state"] == "provisioning":
            self._mark_running(row)  # a very fast job: the state machine requires provisioning -> running -> succeeded
            row = self.get_row(job_id)
            reached_running = True
        if outcome == "cancelled" and row["state"] == "running":
            self.transition(job_id, "cancel_requested", "the process was cancelled externally", expect=("running",), fields={"cancel_requested_at": now_iso()})
            row = self.get_row(job_id)
        tail = ""
        if outcome == "failed" and self.logs.exists(job_id):
            try:
                tail = self.logs.read(job_id, 0, 8000, "stderr", tail=True).data
            except OlcfError:
                tail = ""
        now = now_iso()
        fields: dict[str, Any] = {}
        sp = (result.get("spark") or {}) if isinstance(result, dict) else {}
        if sp.get("application_id") and not row["application_id"]:
            fields["application_id"] = sp["application_id"]
        exit_value = result.get("exit_value") if isinstance(result, dict) else None
        if exit_value is not None and len(json.dumps(exit_value, default=str)) > MAX_RESULT_VALUE_BYTES:
            exit_value = {"truncated": True, "bytes": len(json.dumps(exit_value, default=str))}
        attempt_vals: dict[str, Any] = {"state": outcome, "finished_at": now, "exit_code": exit_code, "process_confirmed_gone": bool(confirmed_gone)}
        new_row: dict[str, Any] | None
        if outcome == "failed":
            cls = classify_failure(failure, exit_code=exit_code, log_tail=tail)
            fail = cls.as_failure(failure)
            if infra_loss and not fail.get("category"):
                fail["category"] = FailureCategory.PROVIDER.value
            attempt_vals["failure"] = fail
            policy = (row["spec"] or {}).get("policy") or {}
            dec = retry_decision(attempt=row["attempt"], max_attempts=row["max_attempts"], category=fail["category"], idempotent=bool(policy.get("idempotent")),
                                 reached_running=bool(reached_running), infrastructure_loss=infra_loss, confirmed_gone=confirmed_gone,
                                 backoff_base_seconds=int(policy.get("retry_backoff_seconds", 30)))
            self._end_attempt(row, attempt_vals)
            if dec.retry:
                new_row = self._requeue(row, fail, dec.backoff_seconds, dec.reason, reason)
                return new_row
            msg = reason or fail["message"]
            if row["max_attempts"] > 1 and not dec.retry:
                msg = f"{msg} ({dec.reason})"
            new_row = self.transition(job_id, "failed", msg, fields={**fields, "failure": fail, "failure_category": fail["category"], "result": redact_obj(result), "exit_value": exit_value,
                                                                     "wait_reason": None}, event_data={"exit_code": exit_code, "retry": dec.reason})
        elif outcome == "succeeded":
            self._end_attempt(row, attempt_vals)
            new_row = self.transition(job_id, "succeeded", reason or "completed successfully", fields={**fields, "result": redact_obj(result), "exit_value": exit_value, "failure": None, "wait_reason": None},
                                      event_data={"exit_code": exit_code})
        elif outcome == "timed_out":
            fail = {"category": FailureCategory.TIMEOUT.value, "message": reason or "the job exceeded its timeout", "remediation": "increase policy.timeout_seconds or optimise the job"}
            attempt_vals["failure"] = fail
            self._end_attempt(row, attempt_vals)
            new_row = self.transition(job_id, "timed_out", reason or "the job exceeded its timeout", fields={**fields, "failure": fail, "failure_category": "timeout", "result": redact_obj(result), "wait_reason": None})
        else:  # cancelled
            self._end_attempt(row, attempt_vals)
            new_row = self.transition(job_id, "cancelled", reason or "cancelled", fields={**fields, "failure": {"category": FailureCategory.CANCELLATION.value, "message": reason or "cancelled"},
                                                                                           "failure_category": "cancellation", "result": redact_obj(result), "wait_reason": None})
        if new_row is not None:
            self._after_terminal(new_row)
        return new_row

    def _end_attempt(self, row: dict[str, Any], vals: dict[str, Any]) -> None:
        with self.db.begin() as conn:
            conn.execute(update(C.job_attempts).where(C.job_attempts.c.job_id == row["id"], C.job_attempts.c.attempt == row["attempt"]).values(**vals))
        self._revoke_job_tokens_for_attempt(row)

    def _revoke_job_tokens_for_attempt(self, row: dict[str, Any]) -> None:
        with self.db.connect() as conn:
            r = conn.execute(select(C.job_attempts.c.token_id).where(C.job_attempts.c.job_id == row["id"], C.job_attempts.c.attempt == row["attempt"])).first()
        if r and r[0]:
            try:
                self.ctx.tokens.revoke(r[0])
            except Exception:  # noqa: BLE001
                log.debug("token revoke failed", exc_info=True)

    def _requeue(self, row: dict[str, Any], failure: dict[str, Any], backoff: int, why: str, reason: str | None) -> dict[str, Any] | None:
        nxt = row["attempt"] + 1
        self.logs.mark(row["id"], f"--- attempt {row['attempt']} failed ({failure.get('category')}): {redact_text(str(failure.get('message')))[:200]}; {why} ---")
        self._cleanup_remote(row)
        new = self.transition(row["id"], "queued", f"{why}: {redact_text(reason or str(failure.get('message')))}"[:500], expect=("provisioning", "running", "cancel_requested"),
                              fields={"attempt": nxt, "driver_worker_id": None, "cluster_id": None, "application_id": None, "started_at": None, "timeout_at": None, "provider_handle": None,
                                      "resources_granted": {}, "not_before": iso_in(backoff) if backoff else None, "lost_since": None, "queued_at": now_iso(), "failure": failure,
                                      "timed_out": False, "provider": (((row["spec"] or {}).get("policy") or {}).get("placement") or {}).get("provider")},
                              event_data={"next_attempt": nxt, "backoff_seconds": backoff, "failure_category": failure.get("category")})
        self.wake()
        return new

    def _after_terminal(self, row: dict[str, Any]) -> None:
        self._cleanup_remote(row)
        try:
            self.logs.close(row["id"])
        except Exception:  # noqa: BLE001
            log.debug("log close failed", exc_info=True)
        with self.db.begin() as conn:
            conn.execute(update(C.spark_apps).where(C.spark_apps.c.job_id == row["id"], C.spark_apps.c.state.in_(("RUNNING", "WAITING"))).values(
                state={"succeeded": "FINISHED", "failed": "FAILED", "cancelled": "KILLED", "timed_out": "KILLED"}.get(row["state"], "FINISHED"), finished_at=now_iso(), updated_at=now_iso()))
        self._metric("olcf_job_duration_seconds", self._duration(row), {"state": row["state"], "kind": row["kind"]})

    @staticmethod
    def _duration(row: dict[str, Any]) -> float | None:
        if row.get("started_at") and row.get("finished_at"):
            return (from_iso(row["finished_at"]) - from_iso(row["started_at"])).total_seconds()
        return None

    def _cleanup_remote(self, row: dict[str, Any]) -> None:
        if not row.get("provider") or not row.get("provider_handle"):
            return
        try:
            p = self.providers.get(row["provider"])
        except OlcfError:
            return
        fn = getattr(p, "cleanup", None)
        if callable(fn):
            try:
                fn(AppHandle.from_job(row))
            except Exception:  # noqa: BLE001
                log.warning("provider cleanup for %s failed", row["id"], exc_info=True)

    # =================================================================== cancel / retry
    def cancel(self, principal: Principal | None, job_id: str, *, reason: str = "cancelled by user") -> JobRecord:
        """Idempotent cancellation (queued: immediate; running: graceful ``cancel_job`` then forced by :meth:`maintain`)."""
        row = self.get_row(job_id)
        if principal is not None:
            principal.require("job.cancel", workspace_id=row["workspace_id"], item_id=row["item_id"])
        if row["state"] in TERMINAL or row["state"] == "cancel_requested":
            return self.to_record(row)
        if principal is not None:
            self.ctx.audit.record(principal, "job.cancel", resource_type="job", resource_id=job_id, workspace_id=row["workspace_id"], details={"state": row["state"], "reason": reason})
        if row["state"] in PRE_RUN:
            new = self.transition(job_id, "cancelled", reason, expect=PRE_RUN, fields={"failure": {"category": "cancellation", "message": reason}, "failure_category": "cancellation", "wait_reason": None})
            if new is not None:
                self._after_terminal(new)
                return self.to_record(new)
            row = self.get_row(job_id)
        new = self.transition(job_id, "cancel_requested", reason, expect=("provisioning", "running"), fields={"cancel_requested_at": now_iso()})
        if new is not None:
            self._send_cancel(new, force=False)
        return self.get(job_id)

    def _send_cancel(self, row: dict[str, Any], *, force: bool) -> None:
        def go() -> None:
            try:
                handle = AppHandle.from_job(row)
                if not (row.get("provider_handle") or {}).get("sent") and not row.get("started_at"):
                    # never reached the provider (the application never reported 'running'): nothing to stop; the launch thread will see the state change
                    if row["state"] == "cancel_requested":
                        fresh = self.get_row(row["id"])
                        if not (fresh.get("provider_handle") or {}).get("sent") and not fresh.get("started_at"):
                            self._finish(fresh, "cancelled", confirmed_gone=True, reason="cancelled before it was started")
                        return
                self.providers.get(row["provider"]).cancel(handle, force=force, grace_seconds=self.grace_cancel)
            except OlcfError as exc:
                log.warning("cancel of %s via provider failed: %s", row["id"], exc.message)
            except Exception:  # noqa: BLE001
                log.exception("cancel of %s failed", row["id"])
        self._pool.submit(go)

    def retry(self, principal: Principal, job_id: str) -> JobRecord:
        """Manual re-run of a failed/cancelled/timed-out job as a *new* job (``retry_of``) with the identical source and pinned runtime."""
        row = self.authorize(principal, job_id, "job.retry")
        if row["state"] not in ("failed", "cancelled", "timed_out"):
            raise Conflict(f"only failed, cancelled or timed-out jobs can be retried (job is {row['state']})")
        d = row["spec"] or {}
        s = d.get("source") or {}
        data = self._read_source(row)
        src = JobSource(format=s.get("format") or "ipynb", content=data.decode("utf-8") if data is not None else None, artifact_uri=s.get("artifact_uri") if data is None else None,
                        filename=s.get("filename"))
        policy = dict(d.get("policy") or {})
        rt = dict(d.get("runtime_request") or {})
        if row["runtime_id"]:
            rt["runtime_id"] = row["runtime_id"]
        from olcf.core.models import PolicySpec, ResourceRequest, RuntimeRequest

        sub = JobSubmission(kind=JobKind(row["kind"]), workspace_id=row["workspace_id"], name=row["name"], item_id=row["item_id"], source=src, parameters=row["parameters"] or {},
                            runtime=RuntimeRequest.model_validate({**rt, "spark_conf": d.get("spark_conf") or {}}), resources=ResourceRequest.model_validate(row["resources_request"] or {}),
                            policy=PolicySpec.model_validate({k: v for k, v in policy.items() if k in PolicySpec.model_fields}),
                            origin=Origin(kind=OriginKind(row["origin_kind"]), details=row["origin"] or {}), parent_run_id=row["parent_run_id"], engine=row["engine"],
                            env=dict(d.get("env") or {}), secret_refs=dict(d.get("secret_refs") or {}), tags=dict(d.get("tags") or {}),
                            default_lakehouse=LakehouseRef(lakehouse_id=(d.get("default_lakehouse") or {}).get("id")) if d.get("default_lakehouse") else None)
        rec = self.submit(principal, sub, internal=row["workspace_id"] == SYSTEM_WORKSPACE)
        with self.db.begin() as conn:
            conn.execute(update(C.jobs).where(C.jobs.c.id == rec.id).values(retry_of=job_id))
        self.add_event(job_id, "retried_as", {"job_id": rec.id})
        return self.get(rec.id)

    # =================================================================== recovery on start / leader change
    def _on_hello(self, worker: dict[str, Any], hello: P.Hello) -> None:
        """Reconcile an agent's reported state with the database (CONTRACTS 4.2)."""
        wid = worker["id"]
        reported = {a.job_id: a for a in hello.active_jobs}
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.driver_worker_id == wid, C.jobs.c.state.in_(ACTIVE_JOB_STATES)))
        known = {r["id"] for r in rows}
        for r in rows:
            a = reported.get(r["id"])
            if a is not None:
                if a.attempt is not None and a.attempt != r["attempt"]:
                    continue
                if r["state"] == "provisioning":
                    self._mark_running(r)
                else:
                    self._update(r["id"], lost_since=None)
                continue
            sent_at = (r.get("provider_handle") or {}).get("sent_at")
            if sent_at and (time.time() - from_iso(sent_at).timestamp()) < 2.0:
                continue  # run_job still in flight (the agent has not started the process yet)
            if r["state"] == "provisioning" and not (r.get("provider_handle") or {}).get("sent"):
                continue  # the launch thread has not sent run_job yet
            # the agent restarted and no longer has the process: it is confirmed gone (an agent re-sends terminal states *before* hello completes)
            self.add_event(r["id"], "agent_lost_job", {"worker_id": wid, "reason": "the agent reconnected without this job"})
            if r["state"] == "cancel_requested":
                self._finish(r, "cancelled", confirmed_gone=True, reason="cancelled (the agent no longer runs the job)")
            else:
                self._finish(r, "failed", failure={"category": "provider", "message": "the worker agent restarted and lost the job process",
                                                   "remediation": "mark the job idempotent (policy.idempotent) to allow automatic replay"},
                             infra_loss=True, confirmed_gone=True, reason="the worker agent reconnected without the job")
        orphans = [jid for jid in reported if jid not in known]
        notes = dict(worker.get("notes") or {})
        if orphans:
            for jid in orphans:
                # a process the controller does not (or no longer) track: never let it keep running unobserved
                try:
                    row = self.get_row(jid)
                    if row["state"] not in TERMINAL and row["driver_worker_id"] == wid:
                        continue
                except NotFound:
                    pass
                # never block the hello handler on the agent (it answers commands only after it has seen the hello acknowledgement)
                self._pool.submit(self.hub.notify, wid, "cancel_job", {"job_id": jid, "attempt": reported[jid].attempt, "grace_seconds": 0, "force": True})
            notes["orphans"] = {"jobs": orphans, "at": now_iso()}
        elif "orphans" in notes:
            notes.pop("orphans")
        if notes != (worker.get("notes") or {}):
            self.registry.update_worker(wid, notes=notes)
        self.wake()

    def recover(self) -> dict[str, int]:
        """Deterministic repair of transient states after a control-plane restart or a leader change (idempotent, safe to repeat)."""
        counts = {"requeued": 0, "failed": 0, "cancelled": 0}
        now = time.time()
        stuck_after = float(self._s("compute.stuck_state_seconds", 300))
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(("accepted", "validating", "provisioning", "cancel_requested"))))
        for r in rows:
            age = now - from_iso(r["updated_at"]).timestamp()
            h = r.get("provider_handle") or {}
            if r["id"] in self._launching:
                continue
            if r["state"] in ("accepted", "validating") and age > stuck_after:
                if self.transition(r["id"], "failed", "the submission was interrupted by a control-plane restart", fields={"failure": {"category": "internal", "message": "submission interrupted"},
                                                                                                                          "failure_category": "internal"}, expect=("accepted", "validating")):
                    counts["failed"] += 1
            elif r["state"] == "provisioning" and not h.get("sent") and h.get("intent_at") and age > stuck_after:
                # the control plane died while a create request to a remote system may have been in flight: do not queue it again (that would create a second remote
                # object), let the monitor look the object up by the attempt's name
                self._update(r["id"], provider_handle={**h, "sent": True, "sent_at": now_iso(), "submission": "uncertain", "uncertain_since": time.time(), "log_seq": 0, "log_offset": 0})
                self.add_event(r["id"], "remote_reconcile", {"code": "submission_uncertain", "detail": "the control plane restarted while the create request was in flight", "provider": r["provider"]})
            elif r["state"] == "provisioning" and not h.get("sent") and age > stuck_after:
                # claimed but never handed to a provider: nothing can have run, safe to queue again
                if self.transition(r["id"], "queued", "re-queued: the control plane restarted before the job was started", expect=("provisioning",),
                                   fields={"driver_worker_id": None, "resources_granted": {}, "provider_handle": None, "queued_at": now_iso()}):
                    counts["requeued"] += 1
            elif r["state"] == "cancel_requested" and not h.get("sent") and age > stuck_after:
                if self._finish(r, "cancelled", confirmed_gone=True, reason="cancelled before it was started"):
                    counts["cancelled"] += 1
        return counts

    # =================================================================== maintenance (leader, every scheduler tick)
    def maintain(self) -> None:
        now = time.time()
        self._enforce_timeouts(now)
        self._escalate_cancels(now)
        self._worker_loss(now)
        self._monitor_remote()
        if now - self._last_slow > 10:
            self._last_slow = now
            self._missing_reports(now)
            self._stuck_provisioning(now)
            self._queue_wait_limits(now)
            self.recover()
        if now - self._last_retention > 3600:
            self._last_retention = now
            try:
                self.purge_retention()
            except Exception:  # noqa: BLE001
                log.exception("retention purge failed")

    def _active(self, states: tuple[str, ...] = ACTIVE_JOB_STATES) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(states)))

    def _enforce_timeouts(self, now: float) -> None:
        stamp = now_iso()
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.state == "running", C.jobs.c.timeout_at.is_not(None), C.jobs.c.timeout_at < stamp, C.jobs.c.timed_out.is_(False)))
        for r in rows:
            self._update(r["id"], timed_out=True, state_reason="timeout exceeded; stopping the job")
            self.add_event(r["id"], "timeout", {"timeout_at": r["timeout_at"]})
            self._send_cancel(r, force=False)
        with self.db.connect() as conn:  # timed out and the agent did not stop it within the grace period
            overdue = fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(("running", "cancel_requested")), C.jobs.c.timed_out.is_(True)))
        for r in overdue:
            if (now - from_iso(r["timeout_at"]).timestamp()) > 2 * self.grace_cancel + 5:
                self._force_or_finish(r, now, "the job exceeded its timeout", "timed_out")

    def _escalate_cancels(self, now: float) -> None:
        for r in self._active(("cancel_requested",)):
            if r["timed_out"] or not r["cancel_requested_at"]:
                continue
            waited = now - from_iso(r["cancel_requested_at"]).timestamp()
            if waited > self.grace_cancel + 5:
                self._force_or_finish(r, now, "cancelled by user", "cancelled", waited=waited)

    def _force_or_finish(self, r: dict[str, Any], now: float, reason: str, outcome: str, *, waited: float | None = None) -> None:
        h = dict(r.get("provider_handle") or {})
        worker = None
        if r["driver_worker_id"]:
            try:
                worker = self.registry.get_worker(r["driver_worker_id"])
            except NotFound:
                worker = None
        online = bool(worker and worker["state"] in ("online", "degraded"))
        if not h.get("cancel_force_sent") and (online or r["provider"] in self.providers.remote_names()):
            self._handle_update(r["id"], cancel_force_sent=True, cancel_force_at=now)
            self.add_event(r["id"], "cancel_forced", {"reason": reason})
            self._send_cancel(self.get_row(r["id"]), force=True)
            return
        forced_at = float(h.get("cancel_force_at") or 0)
        if online and h.get("cancel_force_sent") and now - forced_at < self.grace_cancel + 5:
            return
        confirmed = not online  # an offline worker will kill orphans when it reconnects (hello reconcile)
        self._finish(r, outcome, confirmed_gone=confirmed, reason=f"{reason} ({'worker offline; the process is stopped when the agent reconnects' if not online else 'forced'})")

    def _worker_loss(self, now: float) -> None:
        grace = float(self._s("compute.worker_lost_grace_seconds", 180))
        agent_rows = [r for r in self._active() if r["driver_worker_id"]]
        if not agent_rows:
            return
        with self.db.connect() as conn:
            workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.state, C.workers.c.name))}
        for r in agent_rows:
            w = workers.get(r["driver_worker_id"])
            lost = w is None or w["state"] in ("offline", "revoked")
            if not lost:
                if r["lost_since"]:
                    self._update(r["id"], lost_since=None)
                continue
            if not r["lost_since"]:
                self._update(r["id"], lost_since=now_iso())
                self.add_event(r["id"], "worker_lost", {"worker_id": r["driver_worker_id"], "grace_seconds": grace})
                continue
            if now - from_iso(r["lost_since"]).timestamp() < (0 if (w and w["state"] == "revoked") else grace):
                continue
            name = w["name"] if w else r["driver_worker_id"]
            if r["state"] == "cancel_requested":
                self._finish(r, "cancelled", confirmed_gone=False, reason=f"cancelled; worker {name} is offline")
            elif r["timed_out"]:
                self._finish(r, "timed_out", confirmed_gone=False, reason="timed out; the worker is offline")
            else:
                self._finish(r, "failed", failure={"category": "provider", "message": f"worker {name} was lost and did not return within {int(grace)}s",
                                                   "remediation": "check the node; set policy.idempotent=true to allow automatic replay once the agent confirms the process is gone"},
                             infra_loss=True, confirmed_gone=False, reason=f"worker {name} was lost")

    def _missing_reports(self, now: float) -> None:
        """A running job that a *fresh* heartbeat no longer lists (terminal state lost in transit): treat the process as confirmed gone."""
        grace = float(self._s("compute.missing_report_grace_seconds", 30))
        hb = float(self.ctx.settings.compute.worker_heartbeat_seconds)
        with self.db.connect() as conn:
            workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.state, C.workers.c.active_jobs, C.workers.c.last_seen_at, C.workers.c.connected))}
        for r in self._active(("running",)):
            w = workers.get(r["driver_worker_id"] or "")
            if not w or w["state"] not in ("online", "degraded") or not w["last_seen_at"]:
                continue
            if now - from_iso(w["last_seen_at"]).timestamp() > 3 * hb:
                continue
            reported = {a.get("job_id") for a in (w["active_jobs"] or []) if isinstance(a, dict)}
            if r["id"] in reported:
                continue
            since = from_iso(r["started_at"] or r["updated_at"]).timestamp()
            if now - since < grace + 2 * hb:
                continue
            self._finish(r, "failed", failure={"category": "provider", "message": "the worker agent no longer reports the job and sent no final state"}, infra_loss=True, confirmed_gone=True,
                         reason="the agent no longer reports the job")

    def _stuck_provisioning(self, now: float) -> None:
        limit = float(self._s("compute.provision_timeout_seconds", 300))
        for r in self._active(("provisioning",)):
            h = r.get("provider_handle") or {}
            if not h.get("sent") or r["provider"] in self.providers.remote_names():
                continue
            if now - from_iso(h.get("sent_at") or r["updated_at"]).timestamp() > limit:
                self._finish(r, "failed", failure={"category": "provider", "message": f"the agent accepted the job but it did not start within {int(limit)}s"}, infra_loss=True,
                             confirmed_gone=False, reached_running=False, reason="the job did not start in time")

    def _queue_wait_limits(self, now: float) -> None:
        for q in self.registry.list_queues():
            limit = q.get("max_queue_wait_seconds")
            if not limit:
                continue
            cutoff = iso_in(-float(limit))
            with self.db.connect() as conn:
                rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.state == "queued", C.jobs.c.queue == q["name"], C.jobs.c.queued_at < cutoff))
            for r in rows:
                new = self.transition(r["id"], "timed_out", f"waited longer than the queue limit ({limit}s) for resources", expect=("queued",),
                                      fields={"failure": {"category": "timeout", "message": f"queue wait exceeded {limit}s: {r.get('wait_reason') or 'no capacity'}"}, "failure_category": "timeout"})
                if new:
                    self._after_terminal(new)

    # ---- remote providers (kubernetes, fabric/livy) -------------------------------------------------------------
    def _monitor_remote(self) -> None:
        remote = self.providers.remote_names()
        if not remote:
            return
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(ACTIVE_JOB_STATES), C.jobs.c.provider.in_(sorted(remote))))
        futs = [self._remote_pool.submit(self._poll_remote, r) for r in rows if (r.get("provider_handle") or {}).get("sent")]
        for f in futs:
            try:
                f.result(timeout=60)
            except Exception:  # noqa: BLE001
                log.exception("remote job poll failed")

    #: Keys of ``provider_handle`` owned by the monitor; everything else is the provider's own state and is *replaced* by ``AppStatus.native`` (a provider may drop keys).
    _MONITOR_KEYS = ("sent", "sent_at", "log_seq", "log_offset", "log_cursor", "log_pending", "result", "status_errors", "event_seq", "reconcile_code", "drain_started", "result_wait_since")

    def _poll_remote(self, row: dict[str, Any]) -> None:
        """One monitor pass over a remote-provider job: status, bounded log/result ingestion, then - once the application has ended *and* its output was drained -
        the single completion decision (CONTRACTS, PROV addenda: remote completion contract)."""
        job_id = row["id"]
        h = dict(row.get("provider_handle") or {})
        try:
            provider = self.providers.get(row["provider"])
            st = provider.status(AppHandle.from_job(row))
            if st.native:
                h = {**{k: v for k, v in h.items() if k in self._MONITOR_KEYS}, **st.native}
            h["status_errors"] = 0
            terminal = st.state in TERMINAL_APP_STATES
            drained = self._drain_remote_logs(row, provider, AppHandle.from_job({**row, "provider_handle": h}), h)
            if st.application_id and not row["application_id"]:
                self._update(job_id, application_id=st.application_id)
            self._update(job_id, provider_handle=h)
        except Exception as exc:  # noqa: BLE001
            h["status_errors"] = int(h.get("status_errors", 0)) + 1
            self._update(job_id, provider_handle=h)
            if h["status_errors"] >= int(self._s("compute.remote_status_error_limit", 30)):
                msg = exc.message if isinstance(exc, OlcfError) else f"{type(exc).__name__}: {exc}"
                self._finish(self.get_row(job_id), "failed", failure={"category": "provider", "message": f"lost contact with the {row['provider']} provider: {msg}"}, infra_loss=True,
                             confirmed_gone=False, reason="lost contact with the provider")
            return
        cur = self.get_row(job_id)
        if cur["state"] in TERMINAL:
            return
        if st.code and h.get("reconcile_code") != st.code:  # an operator-visible reason, once per change (e.g. a create request whose answer was lost)
            h["reconcile_code"] = st.code
            self._update(job_id, provider_handle=h)
            self.add_event(job_id, "remote_reconcile", {"code": st.code, "detail": redact_text(st.detail)[:300], "provider": row["provider"]})
            self.logs.mark(job_id, f"--- {redact_text(st.detail)[:300]} ---")
        if st.state == "running" or (terminal and cur["state"] == "provisioning" and st.reached_running is not False):
            self._mark_running(cur)
            cur = self.get_row(job_id)
        if not terminal:
            return
        now = time.time()
        if not drained:
            # the application ended but more of its output is waiting: finishing now would drop the tail (and with it the result). Keep reading on the next polls.
            started = float(h.get("drain_started") or 0)
            if not started:
                h["drain_started"] = now
                self._update(job_id, provider_handle=h)
                return
            if now - started < float(self._s("compute.remote_drain_timeout_seconds", 300)):
                return
            note = "log ingestion stopped before the end of the remote output (drain timeout); the lines after this point are missing"
            self.logs.mark(job_id, f"--- olcf: {note} ---")
            self.add_event(job_id, "log_gap", {"reason": "drain_timeout", "detail": note})
        result_doc = self._completion_document(provider, h, st)
        if st.state == "succeeded":
            if result_doc is None and provider.result_contract == "runner":
                result_doc = self._recover_result(provider, row, h, st)
                if result_doc is None:
                    self._await_result(cur, h, st)
                    return
            if (result_doc or {}).get("state") == "failed":  # the pod exited 0 but the runner says it failed: the runner knows better
                self._finish(cur, "failed", failure=dict(result_doc.get("failure") or {"message": "the runner reported a failure"}), exit_code=st.exit_code, result=result_doc,
                             confirmed_gone=True, reason="the runner reported a failure")
            else:
                self._finish(cur, "succeeded", exit_code=st.exit_code or 0, result=result_doc or {"state": "succeeded"})
        elif st.state == "failed":
            doc = result_doc or {}
            fail = dict(doc.get("failure") or self._failure_from_termination(st) or {"message": st.detail or "the application failed"})
            self._finish(cur, "failed", failure=fail, exit_code=st.exit_code, result=doc, confirmed_gone=True if st.confirmed_gone is None else st.confirmed_gone,
                         reached_running=st.reached_running, reason=st.detail or None)
        elif st.state == "cancelled":
            self._finish(cur, "cancelled", exit_code=st.exit_code, reason=st.detail or "cancelled")

    def _drain_remote_logs(self, row: dict[str, Any], provider: Any, handle: AppHandle, h: dict[str, Any]) -> bool:
        """Ingest up to ``compute.remote_log_pages_per_poll`` pages. The offset and the provider's opaque cursor are persisted after *every* page (a controller restart
        resumes exactly there; the log store ignores a repeated ``seq``). Returns True when the provider reported the end of the retained output."""
        job_id = row["id"]
        for _ in range(max(1, int(self._s("compute.remote_log_pages_per_poll", 16)))):
            offset, cursor = int(h.get("log_offset", 0)), h.get("log_cursor")
            pending = h.get("log_pending")  # a page whose bytes may or may not have reached the log store before the last stop
            page = self._replay_pending_page(provider, handle, offset, cursor, pending) if isinstance(pending, dict) else provider.logs(handle, offset, 262144, cursor=cursor)
            if page.gap:
                self.logs.mark(job_id, f"--- olcf: {page.gap} ---")
                self.add_event(job_id, "log_gap", {"reason": "remote_log_truncated", "detail": page.gap[:300]})
            if page.data:
                if isinstance(pending, dict):
                    h["log_seq"] = int(pending["seq"])  # the store ignores a sequence number it already holds, so a repeated page is not written twice
                else:
                    h["log_seq"] = int(h.get("log_seq", 0)) + 1
                    h["log_pending"] = {"seq": h["log_seq"], "chars": len(page.data)}
                    self._update(job_id, provider_handle=h)
                self.logs.append(job_id, "stdout", h["log_seq"], page.data)
                self._scan_remote_events(row, h, page.data)
            h.pop("log_pending", None)
            h["log_offset"] = page.next_offset
            if page.cursor is not None:
                h["log_cursor"] = page.cursor
            self._update(job_id, provider_handle=h)
            handle.native = dict(h)
            if page.eof or not (page.data or page.next_offset != offset or page.cursor != cursor):
                return True
        return False

    @staticmethod
    def _replay_pending_page(provider: Any, handle: AppHandle, offset: int, cursor: Any, pending: dict[str, Any]) -> Any:
        """Re-read exactly the page that was in flight when the monitor last stopped: whole lines from the saved cursor, ``pending['chars']`` characters in total (the
        page was cut at a line boundary, so asking for the remainder each time reproduces it however the provider slices its reads). The caller appends it under the
        same sequence number, which the log store ignores if the first attempt got through."""
        need, data, gap = int(pending["chars"]), "", None
        cur, off, eof = cursor, offset, False
        while len(data) < need:
            pg = provider.logs(handle, off, need - len(data), cursor=cur)
            gap = gap or pg.gap
            if not pg.data:
                eof = pg.eof
                break
            data += pg.data
            cur, off, eof = pg.cursor, pg.next_offset, pg.eof
        return LogsPage(data, off, eof, cursor=cur, gap=gap)

    def _scan_remote_events(self, row: dict[str, Any], h: dict[str, Any], data: str) -> None:
        """``@@OLCF@@`` lines of a remote application: ``result`` (the completion document, accepted only for this job and attempt) and progress events."""
        job_id = row["id"]
        for line in data.splitlines():
            if not line.startswith("@@OLCF@@"):
                continue
            try:
                ev = json.loads(line[8:].strip())
            except ValueError:
                continue
            if not isinstance(ev, dict) or not ev.get("event"):
                continue
            if ev["event"] == "result":
                doc = ev.get("result")
                if isinstance(doc, dict) and ev.get("job_id", job_id) == job_id and ev.get("attempt", row["attempt"]) == row["attempt"]:
                    h["result"] = doc
                continue
            self.handle_job_event(self.get_row(job_id), str(ev["event"]), {k: v for k, v in ev.items() if k != "event"})

    @staticmethod
    def _completion_document(provider: Any, h: dict[str, Any], st: Any) -> dict[str, Any] | None:
        """The result document of a finished application: the runner's ``result`` event (contract ``runner``) or the provider's own (contract ``provider``)."""
        doc = h.get("result") if isinstance(h.get("result"), dict) else None
        if provider.result_contract == "provider" and isinstance(st.result, dict):
            doc = st.result
        return doc

    @staticmethod
    def _failure_from_termination(st: Any) -> dict[str, Any] | None:
        t = st.termination or {}
        f = t.get("failure") if isinstance(t, dict) else None
        return dict(f) if isinstance(f, dict) and f else None

    def _recover_result(self, provider: Any, row: dict[str, Any], h: dict[str, Any], st: Any) -> dict[str, Any] | None:
        """A runner-backed application ended without a result in the ingested output: look again in what the platform still retains, independent of the cursor
        (a rotated or truncated log may hide the event from the incremental reader), and in the compact termination record the runner leaves."""
        doc: dict[str, Any] | None = None
        try:
            doc = provider.find_result(AppHandle.from_job({**row, "provider_handle": h}))
        except Exception:  # noqa: BLE001
            log.warning("result scan of %s failed", row["id"], exc_info=True)
        if doc is None and isinstance(st.termination, dict) and isinstance(st.termination.get("result"), dict):
            doc = st.termination["result"]
        if doc is not None:
            self.add_event(row["id"], "result_recovered", {"source": "log_scan" if h.get("result") is None else "termination_record"})
        return doc

    def _await_result(self, cur: dict[str, Any], h: dict[str, Any], st: Any) -> None:
        """Terminal application, no result: reconcile for ``compute.remote_result_grace_seconds``, then fail - never a synthesized success."""
        job_id, now = cur["id"], time.time()
        since = float(h.get("result_wait_since") or 0)
        if not since:
            h["result_wait_since"] = now
            self._update(job_id, provider_handle=h)
            self.add_event(job_id, "result_incomplete", {"code": "result_missing", "detail": f"{st.detail or 'the application ended'}; waiting for its result"})
            self.logs.mark(job_id, "--- olcf: the application ended but its result has not arrived; reconciling ---")
            return
        grace = float(self._s("compute.remote_result_grace_seconds", 120))
        if now - since < grace:
            return
        self._finish(cur, "failed", infra_loss=True, confirmed_gone=True, reached_running=True, reason="result missing",
                     failure={"category": "provider", "code": "result_missing",
                              "message": f"the application ended ({st.detail or 'terminal'}) but never delivered its result within {int(grace)}s; OLCF does not report success without it",
                              "remediation": "read the driver log of the application and, if the job writes data, check the target before running it again"})

    # =================================================================== retention
    def purge_retention(self, days: int | None = None, batch: int = 500) -> int:
        """Delete terminal jobs older than the retention window with their events, attempts and files, in small batches."""
        days = int(days if days is not None else self.ctx.settings.retention.job_history_days)
        cutoff = iso_in(-days * 86400)
        removed = 0
        while True:
            with self.db.connect() as conn:
                ids = [r[0] for r in conn.execute(select(C.jobs.c.id).where(C.jobs.c.state.in_(TERMINAL), C.jobs.c.finished_at < cutoff).limit(batch)).all()]
            if not ids:
                break
            with self.db.begin() as conn:
                conn.execute(C.job_events.delete().where(C.job_events.c.job_id.in_(ids)))
                conn.execute(C.job_attempts.delete().where(C.job_attempts.c.job_id.in_(ids)))
                conn.execute(C.spark_apps.delete().where(C.spark_apps.c.job_id.in_(ids)))
                conn.execute(C.jobs.delete().where(C.jobs.c.id.in_(ids)))
            for jid in ids:
                shutil.rmtree(self.jobs_dir / jid, ignore_errors=True)
            removed += len(ids)
        return removed

    # =================================================================== bookkeeping used by API / topology
    def queue_stats(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with self.db.connect() as conn:
            for q, st, n, oldest in conn.execute(select(C.jobs.c.queue, C.jobs.c.state, func.count(), func.min(C.jobs.c.queued_at)).where(C.jobs.c.state.in_(("queued", "provisioning", "running", "cancel_requested")))
                                                 .group_by(C.jobs.c.queue, C.jobs.c.state)).all():
                e = out.setdefault(q, {"depth": 0, "running": 0, "oldest_queued_at": None})
                if st == "queued":
                    e["depth"] += n
                    e["oldest_queued_at"] = min(filter(None, [e["oldest_queued_at"], oldest]), default=None)
                else:
                    e["running"] += n
        return out

    def counts(self) -> dict[str, int]:
        with self.db.connect() as conn:
            rows = conn.execute(select(C.jobs.c.state, func.count()).where(C.jobs.c.state.in_(("queued", "provisioning", "running", "cancel_requested"))).group_by(C.jobs.c.state)).all()
        d = {s: n for s, n in rows}
        return {"queued": d.get("queued", 0), "running": d.get("running", 0) + d.get("provisioning", 0) + d.get("cancel_requested", 0)}

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._remote_pool.shutdown(wait=False, cancel_futures=True)


__all__ = ["JobService", "SYSTEM_WORKSPACE", "decode_cursor", "encode_cursor", "new_etag"]
