# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The durable pipeline engine.

Execution model
---------------
* **The database is the state.** A run is a ``pipeline_runs`` row with an immutable definition snapshot; every activity attempt is an ``activity_runs`` row. Rows are
  created lazily when an activity's dependencies are terminal and the unique ``(run_id, scope_key)`` makes creation idempotent. Nothing important lives only in memory,
  so any replica that becomes leader can pick up all in-flight runs.
* **One dispatcher** (leader only) repeatedly *advances* every active run: it creates rows, evaluates dependency conditions, starts activities, polls asynchronous ones,
  enforces timeouts and cancellation, evaluates containers (if / switch / for_each / until) and finalises the run. Blocking and asynchronous activity code runs on a
  bounded thread pool; inline activities (set_variable, fail) run on the dispatcher.
* **Leases.** An activity attempt is claimed with a random ``lease_token`` (compare-and-set). Every write that ends an attempt is conditional on that token, so a
  deposed leader whose thread is still running can never overwrite what its successor recorded.
* **Crash recovery is deterministic.** A ``running`` row whose token is not owned by the current dispatcher is *adopted*: asynchronous activities are resumed from their
  persisted state (a job id, a child run id, a wake-up time, a callback token) - nothing is submitted twice because job submission is idempotent per ``(activity run, attempt)``;
  blocking activities that are declared safe to repeat (``resumable`` such as ``copy``, read-only ``sql`` / ``http`` GET, or ``policy.idempotent``) are executed again as the
  same attempt; every other interrupted activity is failed with code ``interrupted`` and never silently executed a second time.
* **Failure semantics.** A failed activity is *handled* when a dependent that ran waits on its ``failed`` / ``completed`` outcome; the scope (and run) fails when any failure is
  unhandled. ``skipped`` propagates through dependencies whose conditions are not met. ``Completed`` means succeeded or failed.
* **Resume from failure.** A new run in the same group re-uses the successful results of the previous run (matched by activity path, definition hash and the reuse of every
  upstream dependency) and re-executes the rest; effects on pipeline variables of re-used activities are replayed.
* **Cancellation** propagates to running activities (jobs, child runs, SQL statements), prevents new activities from starting and ends the run as ``cancelled``.
* **Timeouts never overlap a retry.** When an attempt exceeds its timeout the engine does *not* conclude it: it signals cancellation (and calls the activity's ``cancel``
  hook, repeatedly, so a job that was not yet submitted when the deadline passed is still cancelled) and waits - up to ``policy.cancelGraceSeconds`` (default 30) - until
  the attempt has actually stopped (the thread returned, or the job reached a terminal state). Only then is it recorded and, if its policy allows, retried. An attempt
  that stopped at a cancellation point is recorded as ``timeout`` (retryable). One that finished anyway (the work ran past its deadline) is recorded as ``timeout`` and
  retried only when it is idempotent, because its effects exist. One that is still running when the grace ends - or that had already handed work to an external system
  and cannot tell whether it was accepted (an HTTP request that was sent but not answered) - is recorded as ``timeout_uncertain`` and retried **only** when the activity is
  idempotent (``policy.idempotent``, or the activity itself says so: GET, read-only SQL, copy, a request carrying an ``Idempotency-Key``). The operator is told through the
  ``pipeline.activity.outcome_uncertain`` event, the error's ``operatorAction`` and a metric; ``POST /v1/pipeline-runs/{id}/resume`` is the explicit decision to run it again.
"""

from __future__ import annotations

import copy
import json
import logging
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, null, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, Conflict, NotFound, OlcfError, ValidationFailed
from olcf.core.ids import ulid
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import from_iso, to_iso
from olcf.db.util import fetch_all, fetch_one
from olcf.observability import correlation
from olcf.pipelines import model as M
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    ActivityRegistry,
    Container,
)
from olcf.pipelines.expressions import EvalContext, ExpressionError, evaluate_value
from olcf.pipelines.schema import activity_runs as A
from olcf.pipelines.schema import pipeline_runs as R

log = logging.getLogger("olcf.pipelines.engine")

ACT_TERMINAL = ("succeeded", "failed", "skipped", "cancelled")
RUN_ACTIVE = ("running", "cancel_requested")
RUN_TERMINAL = ("succeeded", "failed", "cancelled", "skipped")
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_INPUT_BYTES = 256 * 1024
MAX_CHILD_DEPTH = 8
_COND = {"succeeded": ("succeeded",), "failed": ("failed",), "skipped": ("skipped",), "completed": ("succeeded", "failed")}
_LIGHT = [A.c.id, A.c.run_id, A.c.scope_key, A.c.parent_id, A.c.iteration, A.c.name, A.c.type, A.c.state, A.c.attempt, A.c.max_attempts, A.c.def_hash, A.c.reused_from,
          A.c.lease_token, A.c.next_attempt_at, A.c.timeout_at, A.c.cancel_requested, A.c.created_at, A.c.started_at, A.c.finished_at, A.c.updated_at, A.c.duration_ms]
_RUN_COLS = [c for c in R.c if c.name != "definition"]


def lid(prefix: str) -> str:
    """Local ``<prefix>_<ULID>`` identifier (the orchestration prefixes are not in the foundation ``PREFIXES`` table)."""
    return f"{prefix}_{ulid()}"


class LeaseLost(ActivityCancelled):
    """The attempt's lease was taken over (leader change) - the thread must stop writing."""


# ---------------------------------------------------------------------------------------------------------------------------------------- definitions
@dataclass
class ActDef:
    """One activity definition of a run snapshot with its resolved implementation."""

    raw: dict[str, Any]
    name: str
    type: str
    impl: Activity
    deps: list[dict[str, Any]]
    props: dict[str, Any]
    policy: dict[str, Any]
    hash: str
    max_attempts: int

    @property
    def is_container(self) -> bool:
        return isinstance(self.impl, Container)


class RunDef:
    """Parsed (and cached) definition snapshot of a run."""

    def __init__(self, definition: dict[str, Any], registry: ActivityRegistry) -> None:
        self.definition = definition
        self.registry = registry
        self.settings: dict[str, Any] = definition.get("settings") or {}
        self._cache: dict[int, ActDef] = {}
        self._index: dict[str, ActDef] | None = None
        self.root: list[ActDef] = self.wrap_all(definition.get("activities") or [])

    def wrap_all(self, raws: list[dict[str, Any]]) -> list[ActDef]:
        return [self.wrap(r) for r in raws]

    def index(self) -> dict[str, ActDef]:
        """``{static id: ActDef}`` of every activity including nested scopes (static id = container ids and the name joined by ``/``)."""
        if self._index is None:
            idx: dict[str, ActDef] = {}

            def rec(defs: list[ActDef], prefix: str) -> None:
                for d in defs:
                    idx[prefix + d.name] = d
                    if isinstance(d.impl, Container):
                        for _key, inner in d.impl.scopes(d.props):
                            rec(self.wrap_all([x for x in inner or [] if isinstance(x, dict) and x.get("type")]), prefix + d.name + "/")

            rec(self.root, "")
            self._index = idx
        return self._index

    def wrap(self, raw: dict[str, Any]) -> ActDef:
        hit = self._cache.get(id(raw))
        if hit is not None and hit.raw is raw:
            return hit
        impl = self.registry.get(raw["type"])
        if impl is None:
            raise ActivityError(f"activity type '{raw['type']}' is not installed on this control plane", category="configuration", code="unknown_activity_type", retryable=False)
        pol = M.retry_policy(raw, impl)
        d = ActDef(raw=raw, name=raw["name"], type=raw["type"], impl=impl, deps=M.normalize_dependencies(raw.get("dependsOn")), props=raw.get("typeProperties") or {},
                   policy=pol, hash=M.activity_hash(raw), max_attempts=1 if isinstance(impl, Container) else 1 + max(0, int(pol["retries"])))
        self._cache[id(raw)] = d
        return d


@dataclass
class Scope:
    """A list of activities executing together (the run root, or one iteration of a container)."""

    defs: list[ActDef]
    parent_id: str | None
    iteration: int | None
    chain: tuple[tuple[str | None, int | None], ...]
    over: dict[str, Any]
    path: str
    stable: bool = True


@dataclass
class Task:
    """In-memory handle of an activity attempt currently owned by this dispatcher."""

    row_id: str
    token: str
    run_id: str
    d: ActDef
    props: dict[str, Any]
    attempt: int
    expr: EvalContext
    cancel: threading.Event = field(default_factory=threading.Event)
    busy: bool = False
    started: bool = False
    next_poll: float = 0.0
    cancel_sent: bool = False
    cancel_deadline: float | None = None
    resuming: bool = False
    epoch: int = 0
    started_iso: str | None = None
    timed_out: bool = False                 # the timeout fired: the attempt is being cancelled and has not been concluded yet
    timeout_deadline: float | None = None   # monotonic time at which the cancellation grace ends
    timeout_signalled: float | None = None  # monotonic time at which the timeout fired
    next_cancel_hook: float = 0.0


class RunView:
    """The rows of one run as seen by one ``advance`` pass (light columns; heavy ones are fetched on demand)."""

    def __init__(self, engine: PipelineEngine, run: dict[str, Any], rdef: RunDef) -> None:
        self.engine, self.run, self.rdef = engine, run, rdef
        self.rows: dict[tuple[str | None, int | None, str], dict[str, Any]] = {}
        self.by_id: dict[str, dict[str, Any]] = {}
        self.children: dict[str, list[dict[str, Any]]] = {}
        self.paths: dict[str, str] = {}
        self.variables: dict[str, Any] = dict(run.get("variables") or {})
        self.cancelling = run["state"] == "cancel_requested"
        self.prev: dict[str, dict[str, Any]] | None = None
        self._out: dict[str, Any] = {}
        self._err: dict[str, Any] = {}
        self._sd: dict[str, dict[str, Any]] = {}
        self.now_s = engine.now_iso()

    def load(self) -> None:
        with self.engine.db.connect() as conn:
            rows = fetch_all(conn, select(*_LIGHT).where(A.c.run_id == self.run["id"]).order_by(A.c.id))
        for r in rows:
            self.add(r)

    def add(self, r: dict[str, Any]) -> None:
        self.rows[(r["parent_id"], r["iteration"], r["name"])] = r
        self.by_id[r["id"]] = r
        if r["parent_id"]:
            self.children.setdefault(r["parent_id"], []).append(r)
        if r["parent_id"]:
            self.paths[r["id"]] = f"{self.paths.get(r['parent_id'], '')}/{r['iteration']}/{r['name']}"
        else:
            self.paths[r["id"]] = r["name"]

    def get(self, scope: Scope, name: str) -> dict[str, Any] | None:
        return self.rows.get((scope.parent_id, scope.iteration, name))

    def output(self, row_id: str) -> Any:
        if row_id not in self._out:
            with self.engine.db.connect() as conn:
                r = conn.execute(select(A.c.output).where(A.c.id == row_id)).first()
            self._out[row_id] = r[0] if r else None
        return self._out[row_id]

    def error(self, row_id: str) -> Any:
        if row_id not in self._err:
            with self.engine.db.connect() as conn:
                r = conn.execute(select(A.c.error).where(A.c.id == row_id)).first()
            self._err[row_id] = r[0] if r else None
        return self._err[row_id]

    def state_data(self, row_id: str) -> dict[str, Any]:
        if row_id not in self._sd:
            with self.engine.db.connect() as conn:
                r = conn.execute(select(A.c.state_data).where(A.c.id == row_id)).first()
            self._sd[row_id] = dict(r[0] or {}) if r else {}
        return self._sd[row_id]

    def lookup(self, chain: tuple[tuple[str | None, int | None], ...], name: str) -> dict[str, Any] | None:
        for pid, it in chain:
            r = self.rows.get((pid, it, name))
            if r is not None and r["state"] in ACT_TERMINAL:
                return {"output": self.output(r["id"]) if r["state"] == "succeeded" else None, "status": r["state"], "error": self.error(r["id"]) if r["state"] == "failed" else None,
                        "startTime": r.get("started_at"), "endTime": r.get("finished_at"), "durationMs": r.get("duration_ms")}
        return None

    def get_var(self, name: str) -> Any:
        if name not in self.variables:
            raise ExpressionError(f"variable '{name}' is not defined", code="unknown_variable")
        return self.variables[name]


# ---------------------------------------------------------------------------------------------------------------------------------------- the engine
class PipelineEngine:
    """Leader-side executor of pipeline runs. See the module docstring."""

    def __init__(self, ctx: Any, registry: ActivityRegistry, *, clock: Any = None, tick_seconds: float = 0.5, max_workers: int = 16, cancel_grace_seconds: float = 60.0,
                 max_active_runs: int = 500, timeout_cancel_grace_seconds: float = 30.0) -> None:
        self.ctx, self.db, self.registry = ctx, ctx.db, registry
        self._clock = clock
        self.tick_seconds = tick_seconds
        self.cancel_grace = cancel_grace_seconds
        self.timeout_cancel_grace = timeout_cancel_grace_seconds  # default of policy.cancelGraceSeconds
        self.max_active_runs = max_active_runs
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="olcf-pipe")
        self._inflight: dict[str, Task] = {}
        self._defs: dict[str, RunDef] = {}
        self._errors: dict[str, int] = {}
        self._prev: dict[str, dict[str, dict[str, Any]]] = {}
        self._principals: dict[str, tuple[float, Any]] = {}
        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._was_leader = False
        self._epoch = 0
        self.dead = False
        self.last_loop_at: float | None = None  # monotonic time of the last dispatcher iteration (health check)
        self.last_error: str | None = None
        self.hooks: list[Any] = []  # callables fn(run_row, event) invoked after run state changes (service wires triggers / lineage here)

    # ------------------------------------------------------------------------------------------------------------------------------ clock & misc
    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def now_iso(self) -> str:
        return to_iso(self.now())

    def wake(self) -> None:
        self._wake.set()

    def _metrics(self) -> Any:
        return self.ctx.maybe_service("metrics")

    def _count(self, name: str, labels: dict[str, str], help_: str = "") -> None:
        m = self._metrics()
        if m is not None:
            try:
                m.counter(name, help_).inc(1.0, **labels)
            except Exception:  # noqa: BLE001 - metrics must never break execution
                log.debug("metric failed", exc_info=True)

    def _observe(self, name: str, value: float, labels: dict[str, str], help_: str = "") -> None:
        m = self._metrics()
        if m is not None:
            try:
                m.histogram(name, help_).observe(value, **labels)
            except Exception:  # noqa: BLE001
                log.debug("metric failed", exc_info=True)

    def _event(self, type_: str, run: dict[str, Any], extra: dict[str, Any] | None = None) -> None:
        payload = {"run_id": run["id"], "pipeline_id": run["pipeline_id"], "pipeline": run["pipeline_name"], "state": run["state"], "trigger_type": run.get("trigger_type"),
                   "workspace_id": run["workspace_id"], "parent_run_id": run.get("parent_run_id"), "reason": run.get("state_reason"), **(extra or {})}
        try:
            self.ctx.events.publish(type_, payload, workspace_id=run["workspace_id"], subject=run["id"])
        except Exception:  # noqa: BLE001
            log.exception("could not publish %s", type_)

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.dead = False
        self._thread = threading.Thread(target=self._loop, name="olcf-pipeline-engine", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful stop: the dispatcher ends; running attempts keep their lease and are adopted by the next leader (or on restart)."""
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def crash(self) -> None:
        """Test hook: behave like a killed process - stop the dispatcher at once, drop every in-flight handle and never write again."""
        self.dead = True
        self._stop.set()
        self._wake.set()
        for t in list(self._inflight.values()):
            t.cancel.set()
        self._inflight.clear()
        if self._thread:
            self._thread.join(timeout=5)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                leader = self.ctx.leader.is_leader()
                if leader:
                    if not self._was_leader:
                        log.info("pipeline engine became leader; adopting in-flight runs")
                    self._was_leader = True
                    self.tick()
                else:
                    self._was_leader = False
                self.last_error = None
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"{type(exc).__name__}: {redact_text(str(exc))[:300]}"
                log.exception("pipeline engine tick failed")
            self.last_loop_at = time.monotonic()
            self._wake.wait(self.tick_seconds)
            self._wake.clear()

    # ------------------------------------------------------------------------------------------------------------------------------ tick
    def tick(self) -> None:
        """One dispatcher iteration (also callable from tests)."""
        if self.dead:
            return
        self._admit()
        with self.db.connect() as conn:
            ids = [r[0] for r in conn.execute(select(R.c.id).where(R.c.state.in_(RUN_ACTIVE)).order_by(R.c.created_at))]
        active = set(ids)
        for rid in list(self._defs):
            if rid not in active:
                self._defs.pop(rid, None)
                self._errors.pop(rid, None)
        for rid in ids:
            if self.dead:
                return
            try:
                self._advance(rid)
                self._errors.pop(rid, None)
            except Exception as exc:  # noqa: BLE001
                n = self._errors.get(rid, 0) + 1
                self._errors[rid] = n
                log.exception("advancing run %s failed (%d)", rid, n)
                if n >= 5:
                    self._fail_run_internal(rid, f"engine error: {type(exc).__name__}: {redact_text(str(exc))[:400]}")
        self._gauges()

    def _gauges(self) -> None:
        m = self._metrics()
        if m is None:
            return
        try:
            with self.db.connect() as conn:
                rows = conn.execute(select(R.c.state, func.count()).where(R.c.state.in_(("queued", "running", "cancel_requested"))).group_by(R.c.state)).all()
            counts = {s: n for s, n in rows}
            g = m.gauge("olcf_pipeline_runs", "Pipeline runs by state (queued / running)")
            for s in ("queued", "running", "cancel_requested"):
                g.set(float(counts.get(s, 0)), state=s)
            m.gauge("olcf_pipeline_activities_inflight", "Activity attempts currently executing in this control plane").set(float(len(self._inflight)))
        except Exception:  # noqa: BLE001
            log.debug("gauge update failed", exc_info=True)

    # ------------------------------------------------------------------------------------------------------------------------------ run creation
    def definition_of(self, item: dict[str, Any]) -> dict[str, Any]:
        """Validate the stored definition of a pipeline item and return its normalized form (raises :class:`ValidationFailed`)."""
        res = M.validate_definition(item.get("definition") or {}, self.registry)
        res.raise_if_invalid()
        return res.definition

    def create_run(self, principal: Any, item: dict[str, Any], parameters: dict[str, Any] | None = None, trigger: dict[str, Any] | None = None, *,
                   idempotency_key: str | None = None, parent_run: dict[str, Any] | None = None, parent_activity_id: str | None = None,
                   resumed_from: dict[str, Any] | None = None, resume_options: dict[str, Any] | None = None, definition: dict[str, Any] | None = None,
                   queue_policy: str | None = None, trigger_type: str | None = None) -> dict[str, Any]:
        """Insert a ``queued`` run (idempotent per ``idempotency_key``). The dispatcher admits it subject to the pipeline's concurrency settings."""
        if item["type"] != "pipeline":
            raise BadRequest(f"item {item['id']} is a {item['type']}, not a pipeline")
        defn = definition or self.definition_of(item)
        params = M.resolve_parameters(defn, parameters)
        settings = defn.get("settings") or {}
        trig = dict(trigger or {"type": "manual"})
        trig.setdefault("type", "manual")
        trig["principal_scopes"] = list(getattr(principal, "token_scopes", None) or ["*"])
        trig.pop("principal_credential", None)
        if getattr(principal, "workspace_lock", None) is not None:  # F01: a run started by a job / kernel credential keeps that credential's ceiling and workspace binding
            trig["principal_credential"] = principal.credential_ceiling(principal.workspace_lock)
        now = self.now_iso()
        rid = lid("run")
        root = (parent_run or {}).get("root_run_id") or (parent_run or {}).get("id") or rid
        group = (resumed_from or {}).get("group_id") or (resumed_from or {}).get("id") or rid
        row = {
            "id": rid, "workspace_id": item["workspace_id"], "pipeline_id": item["id"], "pipeline_name": item["name"], "pipeline_version": int(item.get("version") or 1),
            "definition": defn, "def_hash": M.sha256_of(defn), "parameters": params, "variables": M.initial_variables(defn), "state": "queued", "trigger": trig,
            "trigger_type": trigger_type or trig["type"], "principal_id": principal.id, "parent_run_id": (parent_run or {}).get("id"), "parent_activity_id": parent_activity_id,
            "root_run_id": root, "depth": int((parent_run or {}).get("depth", -1)) + 1 if parent_run else 0, "resumed_from": (resumed_from or {}).get("id"),
            "resume_options": resume_options or {}, "group_id": group, "idempotency_key": idempotency_key,
            "queue_policy": queue_policy or str(settings.get("queuePolicy") or "queue"), "concurrency": int(settings.get("concurrency") or 0),
            "correlation_id": getattr(principal, "correlation_id", None), "created_at": now, "updated_at": now,
        }
        try:
            with self.db.begin() as conn:
                conn.execute(R.insert().values(**row))
        except IntegrityError:
            if idempotency_key:
                with self.db.connect() as conn:
                    existing = fetch_one(conn, select(*_RUN_COLS).where(R.c.idempotency_key == idempotency_key))
                if existing:
                    return existing
            raise
        out = {k: v for k, v in row.items() if k != "definition"}
        self._event("pipeline.run.queued", out)
        self._count("olcf_pipeline_runs_created_total", {"trigger": str(out["trigger_type"])}, "Pipeline runs created")
        self.wake()
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ admission
    def _admit(self) -> None:
        with self.db.connect() as conn:
            queued = fetch_all(conn, select(*_RUN_COLS).where(R.c.state == "queued").order_by(R.c.created_at, R.c.id).limit(500))
            running_total = conn.execute(select(func.count()).select_from(R).where(R.c.state.in_(RUN_ACTIVE))).scalar() or 0
            per_pipeline = {pid: n for pid, n in conn.execute(select(R.c.pipeline_id, func.count()).where(R.c.state.in_(RUN_ACTIVE)).group_by(R.c.pipeline_id))}
        if not queued:
            return
        cap = int(self._setting("pipelines.max_active_runs", self.max_active_runs))
        for run in queued:
            pid = run["pipeline_id"]
            limit = int(run["concurrency"] or 0)
            running_here = per_pipeline.get(pid, 0)
            if limit and running_here >= limit:
                policy = run["queue_policy"]
                if policy == "skip":
                    self._set_run_state(run, "skipped", f"queuePolicy=skip: the pipeline already has {running_here} active run(s) (concurrency={limit})", finished=True)
                elif policy == "cancel_previous":
                    self._cancel_oldest(pid, run["id"], f"superseded by run {run['id']} (queuePolicy=cancel_previous)")
                continue
            if running_total >= cap:
                continue
            if self._start_run(run):
                per_pipeline[pid] = running_here + 1
                running_total += 1

    def _setting(self, key: str, default: Any) -> Any:
        try:
            st = self.ctx.maybe_service("settings")
            if st is not None and key in st.registry:
                v = st.get(key)
                return default if v is None else v
        except Exception:  # noqa: BLE001
            pass
        return default

    def _cancel_oldest(self, pipeline_id: str, except_run: str, reason: str) -> None:
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(R.c.id).where(R.c.pipeline_id == pipeline_id, R.c.state == "running", R.c.id != except_run).order_by(R.c.created_at).limit(1))
        for r in rows:
            self.cancel_run(r["id"], reason)

    def _start_run(self, run: dict[str, Any]) -> bool:
        now = self.now()
        rdef = self._load_def(run["id"])
        tmo = rdef.settings.get("timeoutSeconds")
        vals: dict[str, Any] = {"state": "running", "started_at": to_iso(now), "updated_at": to_iso(now)}
        if tmo:
            vals["deadline_at"] = to_iso(now + timedelta(seconds=float(tmo)))
        with self.db.begin() as conn:
            n = conn.execute(update(R).where(R.c.id == run["id"], R.c.state == "queued").values(**vals)).rowcount
        if not n:
            return False
        run.update(vals)
        self._event("pipeline.run.started", run)
        return True

    def _load_def(self, run_id: str) -> RunDef:
        rd = self._defs.get(run_id)
        if rd is None:
            with self.db.connect() as conn:
                r = conn.execute(select(R.c.definition).where(R.c.id == run_id)).first()
            if not r:
                raise NotFound(f"run {run_id} not found")
            rd = RunDef(r[0], self.registry)
            self._defs[run_id] = rd
        return rd

    # ------------------------------------------------------------------------------------------------------------------------------ run state helpers
    def _set_run_state(self, run: dict[str, Any], state: str, reason: str | None, *, finished: bool = False, **extra: Any) -> bool:
        now = self.now_iso()
        vals: dict[str, Any] = {"state": state, "state_reason": (reason or "")[:500] or None, "updated_at": now, **extra}
        if finished:
            vals["finished_at"] = now
        with self.db.begin() as conn:
            n = conn.execute(update(R).where(R.c.id == run["id"], R.c.state.notin_(RUN_TERMINAL)).values(**vals)).rowcount
        if n:
            run.update(vals)
            if finished:
                self._on_run_finished(run)
        return bool(n)

    def _on_run_finished(self, run: dict[str, Any]) -> None:
        st = run["state"]
        self._count("olcf_pipeline_runs_total", {"state": st, "pipeline": run["pipeline_name"][:64]}, "Finished pipeline runs by state")
        try:
            if run.get("started_at") and run.get("finished_at"):
                self._observe("olcf_pipeline_run_duration_seconds", (from_iso(run["finished_at"]) - from_iso(run["started_at"])).total_seconds(), {"state": st},
                              "Pipeline run duration")
        except Exception:  # noqa: BLE001
            pass
        self._event(f"pipeline.run.{st}", run, {"error": run.get("error"), "duration_seconds": _dur(run)})
        for h in list(self.hooks):
            try:
                h(run, st)
            except Exception:  # noqa: BLE001
                log.exception("run hook failed")
        self._defs.pop(run["id"], None)
        self._prev.pop(run["id"], None)
        self._principals.pop(run["id"], None)
        self.wake()

    def _fail_run_internal(self, run_id: str, message: str) -> None:
        with self.db.connect() as conn:
            run = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == run_id))
        if not run or run["state"] in RUN_TERMINAL:
            return
        for t in [t for t in self._inflight.values() if t.run_id == run_id]:
            t.cancel.set()
        self._set_run_state(run, "failed", message, finished=True, error={"code": "internal", "message": message})

    def run_row(self, run_id: str) -> dict[str, Any]:
        """The run row (without the definition snapshot)."""
        with self.db.connect() as conn:
            row = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == run_id))
        if not row:
            raise NotFound(f"pipeline run {run_id} not found")
        return row

    def cancel_run(self, run_id: str, reason: str = "cancelled by user", *, fail_fast: bool = False) -> dict[str, Any]:
        """Request cancellation (idempotent). Queued runs are cancelled at once; running ones are drained by the dispatcher."""
        now = self.now_iso()
        with self.db.begin() as conn:
            row = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == run_id).with_for_update())
            if not row:
                raise NotFound(f"pipeline run {run_id} not found")
            if row["state"] in RUN_TERMINAL or row["state"] == "cancel_requested":
                return row
            if row["state"] == "queued":
                conn.execute(update(R).where(R.c.id == run_id).values(state="cancelled", state_reason=reason[:500], finished_at=now, updated_at=now, cancel_requested_at=now, cancel_reason=reason[:500]))
                row.update(state="cancelled", state_reason=reason[:500], finished_at=now, updated_at=now)
                queued_cancel = True
            else:
                conn.execute(update(R).where(R.c.id == run_id).values(state="cancel_requested", state_reason=reason[:500], cancel_requested_at=now, updated_at=now,
                                                                      cancel_reason=("failfast: " if fail_fast else "") + reason[:480]))
                row.update(state="cancel_requested", state_reason=reason[:500], cancel_requested_at=now)
                queued_cancel = False
        if queued_cancel:
            self._on_run_finished(row)
        self.wake()
        return row

    # ------------------------------------------------------------------------------------------------------------------------------ principals
    def principal_for(self, run: dict[str, Any]) -> Any:
        hit = self._principals.get(run["id"])
        if hit and time.monotonic() - hit[0] < 5.0:
            return hit[1]
        from olcf.db import schema as S

        with self.db.connect() as conn:
            prow = fetch_one(conn, select(S.principals).where(S.principals.c.id == run["principal_id"]))
        if prow is None or not prow.get("is_active", True):
            raise ActivityError("the principal that owns this run no longer exists or is disabled", category="authn", code="principal_disabled", retryable=False)
        scopes = (run.get("trigger") or {}).get("principal_scopes") or ["*"]
        cred = (run.get("trigger") or {}).get("principal_credential")
        p = self.ctx.authz.principal_from_row(prow, token_scopes=list(scopes), credential=cred if isinstance(cred, dict) else None)
        if cred is not None and self.ctx.tokens.derived_origin_revoked(p.origin_token_id, p.origin_family):
            raise ActivityError("the credential this run was started with was revoked", category="authn", code="credential_revoked", retryable=False)
        p.correlation_id = run.get("correlation_id")
        self._principals[run["id"]] = (time.monotonic(), p)
        return p

    # ------------------------------------------------------------------------------------------------------------------------------ advance
    def _advance(self, run_id: str) -> None:
        with self.db.connect() as conn:
            run = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == run_id))
        if run is None or run["state"] not in RUN_ACTIVE:
            return
        rdef = self._load_def(run_id)
        view = RunView(self, run, rdef)
        view.load()
        if run.get("resumed_from"):
            self._load_prev(view)
        now = self.now()
        if run.get("deadline_at") and run["state"] == "running" and run["deadline_at"] < to_iso(now):
            self.cancel_run(run_id, f"run exceeded its timeout of {rdef.settings.get('timeoutSeconds')} s", fail_fast=True)
            run = self.run_row(run_id)
            view.run, view.cancelling = run, True
        root = Scope(defs=rdef.root, parent_id=None, iteration=None, chain=((None, None),), over={}, path="", stable=True)
        status = self._walk(view, root)
        if status != "running":
            self._finalize(view, status)

    def _ectx(self, view: RunView, scope: Scope) -> EvalContext:
        run = view.run
        trig = dict(run.get("trigger") or {})
        trig.pop("principal_scopes", None)
        trig.pop("principal_credential", None)
        pipeline = {"RunId": run["id"], "Pipeline": run["pipeline_name"], "PipelineId": run["pipeline_id"], "Workspace": run["workspace_id"], "TriggerType": run["trigger_type"],
                    "TriggerTime": trig.get("scheduledTime") or run["created_at"], "GroupId": run.get("group_id"), "ParentRunId": run.get("parent_run_id")}
        chain = scope.chain
        return EvalContext(parameters=run.get("parameters") or {}, variables=view.get_var, pipeline=pipeline, trigger=trig, activity=lambda n: view.lookup(chain, n), now=self.now,
                           workspace={"id": run["workspace_id"]}, **scope.over)

    # ------------------------------------------------------------------------------------------------------------------------------ scope walk
    def _walk(self, view: RunView, scope: Scope) -> str:
        defs = scope.defs
        if not view.cancelling:
            changed = True
            while changed:
                changed = False
                for d in defs:
                    if view.get(scope, d.name) is not None:
                        continue
                    verdict = self._verdict(view, scope, d)
                    if verdict == "wait":
                        continue
                    row = self._create_row(view, scope, d, skipped=(verdict == "skip"))
                    if row["state"] in ACT_TERMINAL:
                        changed = True
        for d in defs:
            row = view.get(scope, d.name)
            if row is not None:
                self._drive(view, scope, d, row)
        if not view.cancelling and view.rdef.settings.get("failFast"):
            for d in defs:
                row = view.get(scope, d.name)
                if row is not None and row["state"] == "failed" and not self._handlers(defs, d.name):
                    self.cancel_run(view.run["id"], f"failFast: activity '{d.name}' failed", fail_fast=True)
                    view.run = self.run_row(view.run["id"])
                    view.cancelling = True
                    break
        return self._scope_result(view, scope)

    @staticmethod
    def _handlers(defs: list[ActDef], name: str) -> list[ActDef]:
        out = []
        for x in defs:
            for dep in x.deps:
                if dep["activity"] == name and any(c in ("failed", "completed") for c in dep["conditions"]):
                    out.append(x)
        return out

    def _verdict(self, view: RunView, scope: Scope, d: ActDef) -> str:
        for dep in d.deps:
            r = view.get(scope, dep["activity"])
            if r is None or r["state"] not in ACT_TERMINAL:
                return "wait"
            ok = any(r["state"] in _COND.get(c, ()) for c in dep["conditions"])
            if not ok:
                return "skip"
        return "run"

    def _scope_result(self, view: RunView, scope: Scope) -> str:
        rows = [view.get(scope, d.name) for d in scope.defs]
        if any(r is not None and r["state"] not in ACT_TERMINAL for r in rows):
            return "running"
        if view.cancelling:
            return "cancelled"
        if any(r is None for r in rows):
            return "running"
        for d, r in zip(scope.defs, rows, strict=True):
            if r["state"] in ("failed", "cancelled"):
                handlers = [view.get(scope, h.name) for h in self._handlers(scope.defs, d.name)]
                if not any(h is not None and h["state"] != "skipped" for h in handlers):
                    return "failed"
        return "succeeded"

    # ------------------------------------------------------------------------------------------------------------------------------ rows
    def _row_path(self, scope: Scope, name: str) -> str:
        return scope.path + name

    def _create_row(self, view: RunView, scope: Scope, d: ActDef, *, skipped: bool) -> dict[str, Any]:
        now = self.now_iso()
        rid = lid("act")
        key = f"{scope.parent_id or '-'}|{scope.iteration if scope.iteration is not None else '-'}|{d.name}"
        vals: dict[str, Any] = {
            "id": rid, "run_id": view.run["id"], "scope_key": key, "parent_id": scope.parent_id, "iteration": scope.iteration, "name": d.name, "type": d.type,
            "state": "skipped" if skipped else "ready", "attempt": 0, "max_attempts": d.max_attempts, "def_hash": d.hash, "state_data": {}, "attempts": [], "job_ids": [],
            "cancel_requested": False, "created_at": now, "updated_at": now,
        }
        reuse = None if skipped else self._reuse_candidate(view, scope, d)
        if skipped:
            vals.update(finished_at=now, duration_ms=0)
        elif reuse is not None:
            vals.update(state="succeeded", output=reuse["output"], state_data=reuse.get("state_data") or {}, reused_from=reuse["id"], finished_at=now, started_at=now, duration_ms=0,
                        attempt=0)
        try:
            with self.db.begin() as conn:
                conn.execute(A.insert().values(**vals))
        except IntegrityError:
            with self.db.connect() as conn:
                existing = fetch_one(conn, select(*_LIGHT).where(A.c.run_id == view.run["id"], A.c.scope_key == key))
            if existing is None:
                raise
            view.add(existing)
            return existing
        light = {c.name: vals.get(c.name) for c in _LIGHT}
        view.add(light)
        if reuse is not None:
            effects = (reuse.get("state_data") or {}).get("effects")
            if effects:
                self._apply_effects(view, effects)
            view._out[rid] = reuse["output"]
            self._count("olcf_pipeline_activity_runs_total", {"type": d.type, "state": "reused"}, "Activity runs by type and final state")
        elif skipped:
            self._count("olcf_pipeline_activity_runs_total", {"type": d.type, "state": "skipped"}, "Activity runs by type and final state")
        return light

    def _load_prev(self, view: RunView) -> None:
        if view.prev is not None:
            return
        prev_id = view.run.get("resumed_from")
        cached = self._prev.get(view.run["id"])
        if cached is not None:
            view.prev = cached
            return
        view.prev = {}
        if not prev_id:
            return
        self._prev[view.run["id"]] = view.prev
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(A.c.id, A.c.parent_id, A.c.iteration, A.c.name, A.c.state, A.c.def_hash, A.c.output, A.c.state_data).where(A.c.run_id == prev_id)
                             .order_by(A.c.id))
        paths: dict[str, str] = {}
        for r in rows:
            pp = paths.get(r["parent_id"], "") if r["parent_id"] else ""
            p = (f"{pp}/{r['iteration']}/{r['name']}" if r["parent_id"] else r["name"])
            paths[r["id"]] = p
            if r["state"] == "succeeded":
                view.prev[p] = r

    def _reuse_candidate(self, view: RunView, scope: Scope, d: ActDef) -> dict[str, Any] | None:
        opts = view.run.get("resume_options") or {}
        if not view.run.get("resumed_from") or opts.get("reuse") is False or view.prev is None or not scope.stable:
            return None
        if not d.policy.get("reusable", True):
            return None
        prev = view.prev.get(self._row_path(scope, d.name))
        if prev is None or prev["def_hash"] != d.hash:
            return None
        rerun = set(opts.get("rerun") or [])
        if rerun and (d.name in rerun or _contains_names(d.raw, rerun, view.rdef.registry)):
            return None
        for dep in d.deps:
            r = view.get(scope, dep["activity"])
            if r is None or r.get("reused_from") is None:
                return None
        return prev

    def _apply_effects(self, view: RunView, effects: dict[str, Any]) -> None:
        vs = effects.get("variables") or {}
        if not vs:
            return
        with self.db.begin() as conn:
            cur = conn.execute(select(R.c.variables).where(R.c.id == view.run["id"]).with_for_update()).scalar() or {}
            new = {**cur, **vs}
            conn.execute(update(R).where(R.c.id == view.run["id"]).values(variables=new, updated_at=self.now_iso()))
        view.variables.update(vs)

    # ------------------------------------------------------------------------------------------------------------------------------ driving rows
    def _drive(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any]) -> None:
        if row["state"] in ACT_TERMINAL:
            return
        if d.is_container:
            self._drive_container(view, scope, d, row)
            return
        st = row["state"]
        if st in ("ready", "retry_wait"):
            if view.cancelling:
                self._finish_unleased(view, row, "cancelled", error={"code": "cancelled", "message": view.run.get("state_reason") or "run cancelled"})
                return
            if st == "retry_wait" and row["next_attempt_at"] and row["next_attempt_at"] > to_iso(self.now()):
                return
            if self._running_count(view) >= int(view.rdef.settings.get("maxParallelActivities") or 16):
                return
            self._dispatch(view, scope, d, row)
            return
        # running
        task = self._inflight.get(row["id"])
        if task is None or task.token != row["lease_token"]:
            self._adopt(view, scope, d, row)
            return
        nowiso = self.now_iso()
        if task.timed_out:
            self._drive_timed_out(view, row, task)
            return
        if row["timeout_at"] and row["timeout_at"] < nowiso and not task.cancel.is_set():
            self._time_out(view, row, task)
            return
        if view.cancelling:
            self._cancel_task(view, scope, d, row, task)
            return
        if d.impl.mode == "async" and not task.busy and task.started and time.monotonic() >= task.next_poll:
            task.busy = True
            self._pool.submit(self._run_phase, task, "poll")

    def _running_count(self, view: RunView) -> int:
        return sum(1 for r in view.rows.values() if r["state"] == "running" and r["lease_token"])

    def _finish_unleased(self, view: RunView, row: dict[str, Any], state: str, *, error: dict[str, Any] | None = None, output: Any = None) -> None:
        """End a row that holds no lease (ready / retry_wait / container)."""
        now = self.now_iso()
        vals: dict[str, Any] = {"state": state, "finished_at": now, "updated_at": now, "lease_token": None}
        if error is not None:
            vals["error"] = error
        if output is not None:
            vals["output"] = output
        if row.get("started_at"):
            vals["duration_ms"] = int((from_iso(now) - from_iso(row["started_at"])).total_seconds() * 1000)
        with self.db.begin() as conn:
            n = conn.execute(update(A).where(A.c.id == row["id"], A.c.state.in_(("ready", "retry_wait", "running"))).values(**vals)).rowcount
        if n:
            row.update(vals)
            self._after_activity(view.run, row, state)

    def _announce_uncertain(self, run: dict[str, Any], row: dict[str, Any], *, retried: bool) -> None:
        """Tell the operator that an attempt may have done its work (event + metric + log); see the module docstring, "Timeouts never overlap a retry"."""
        err = row.get("error") or {}
        self._count("olcf_pipeline_activity_outcome_uncertain_total", {"type": row["type"], "category": str(err.get("category"))}, "Activity attempts whose outcome is unknown after a timeout")
        log.warning("pipeline run %s activity '%s' (%s): outcome unknown (%s); %s", run["id"], row["name"], row["type"], err.get("category"),
                    "retried because the activity is idempotent" if retried else "not retried: operator decision needed")
        try:
            self.ctx.events.publish("pipeline.activity.outcome_uncertain", {"run_id": run["id"], "pipeline": run["pipeline_name"], "pipeline_id": run["pipeline_id"], "activity": row["name"],
                                                                           "type": row["type"], "error": redact_obj(err), "attempt": row.get("attempt"), "retried": retried},
                                    workspace_id=run["workspace_id"], subject=run["id"])
        except Exception:  # noqa: BLE001
            log.debug("could not publish the uncertain-outcome event", exc_info=True)

    def _after_activity(self, run: dict[str, Any], row: dict[str, Any], state: str) -> None:
        self._count("olcf_pipeline_activity_runs_total", {"type": row["type"], "state": state}, "Activity runs by type and final state")
        if row.get("duration_ms") is not None and state in ("succeeded", "failed"):
            self._observe("olcf_pipeline_activity_duration_seconds", row["duration_ms"] / 1000.0, {"type": row["type"]}, "Activity duration")
        if state == "failed" and str((row.get("error") or {}).get("category", "")).endswith("_uncertain"):
            self._announce_uncertain(run, row, retried=False)
        if state == "failed":
            err = row.get("error") or {}
            try:
                self.ctx.events.publish("pipeline.activity.failed", {"run_id": run["id"], "pipeline": run["pipeline_name"], "pipeline_id": run["pipeline_id"], "activity": row["name"],
                                                                     "type": row["type"], "error": redact_obj(err), "attempt": row.get("attempt")},
                                        workspace_id=run["workspace_id"], subject=run["id"])
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------------------------------------------------------------------ dispatch
    def _make_input(self, d: ActDef, props: dict[str, Any]) -> Any:
        if d.policy.get("secureInput") or d.raw.get("policy", {}).get("secureInput"):
            return {"secure": True}
        shown = copy.deepcopy(props)
        for path in d.impl.sensitive_props:
            cur: Any = shown
            parts = path.split(".")
            for part in parts[:-1]:
                cur = cur.get(part) if isinstance(cur, dict) else None
            if isinstance(cur, dict) and parts[-1] in cur:
                cur[parts[-1]] = "***"
        shown = redact_obj(shown)
        try:
            if len(json.dumps(shown, default=str)) > MAX_INPUT_BYTES:
                return {"truncated": True}
        except (TypeError, ValueError):
            return {"truncated": True}
        return shown

    def _dispatch(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any]) -> None:
        ectx = self._ectx(view, scope)
        try:
            props = evaluate_value(d.props, ectx)
        except ExpressionError as exc:
            self._fail_row(view, row, ActivityError(f"expression error in '{d.name}': {exc.render()}", category="expression", code=exc.code, retryable=False))
            return
        except OlcfError as exc:
            self._fail_row(view, row, ActivityError(exc.message, category=exc.code, retryable=False))
            return
        token = secrets.token_hex(16)
        attempt = int(row["attempt"]) + 1
        snapshot = EvalContext(parameters=ectx.parameters, variables=dict(view.variables), pipeline=ectx.pipeline, trigger=ectx.trigger, activity=ectx.activity, item=ectx.item,
                               item_index=ectx.item_index, has_item=ectx.has_item, now=ectx.now, workspace=ectx.workspace)
        now = self.now()
        task = Task(row_id=row["id"], token=token, run_id=view.run["id"], d=d, props=props, attempt=attempt, expr=ectx if d.impl.mode == "inline" else snapshot, epoch=self._epoch,
                    started_iso=to_iso(now))
        tmo = self._timeout_for(d, props)
        vals = {"state": "running", "attempt": attempt, "lease_token": token, "started_at": row.get("started_at") or to_iso(now), "timeout_at": to_iso(now + timedelta(seconds=float(tmo))) if tmo else None,
                "input": self._make_input(d, props), "next_attempt_at": None, "updated_at": to_iso(now)}
        self._inflight[row["id"]] = task
        with self.db.begin() as conn:
            n = conn.execute(update(A).where(A.c.id == row["id"], A.c.state.in_(("ready", "retry_wait"))).values(**vals)).rowcount
        if not n:
            self._inflight.pop(row["id"], None)
            return
        row.update({k: v for k, v in vals.items() if k in row})
        row["lease_token"] = token
        if d.impl.mode == "inline":
            self._run_inline(view, task, row)
        else:
            task.busy = True
            self._pool.submit(self._run_phase, task, "exec" if d.impl.mode == "blocking" else "start")

    def _fail_row(self, view: RunView, row: dict[str, Any], err: ActivityError) -> None:
        """Fail an attempt that never got a lease (e.g. expression errors): retry policy does not apply to configuration errors."""
        self._finish_unleased(view, row, "failed", error=err.to_dict())

    def _make_actx(self, task: Task, full: dict[str, Any], run: dict[str, Any], *, resuming: bool = False) -> ActivityContext:
        rid = task.row_id

        def save(state: dict[str, Any]) -> None:
            if self.dead:
                raise LeaseLost()
            with self.db.begin() as conn:
                n = conn.execute(update(A).where(A.c.id == rid, A.c.lease_token == task.token, A.c.state == "running").values(state_data=state, updated_at=self.now_iso())).rowcount
            if not n:
                task.cancel.set()
                raise LeaseLost()

        def link(job_id: str) -> None:
            with self.db.begin() as conn:
                cur = conn.execute(select(A.c.job_ids).where(A.c.id == rid).with_for_update()).scalar() or []
                if job_id not in cur:
                    conn.execute(update(A).where(A.c.id == rid).values(job_ids=[*cur, job_id]))

        principal = self.principal_for(run)
        return ActivityContext(ctx=self.ctx, principal=principal, run=run, row=full, definition=task.d.raw, props=task.props, state=dict(full.get("state_data") or {}), attempt=task.attempt,
                               resuming=resuming, expr=task.expr, engine=self, pipeline_def=self._defs[run["id"]].definition if run["id"] in self._defs else {}, signal=full.get("signal"),
                               now=self.now, _save=save, _link_job=link, _cancel=task.cancel)

    def _fetch_full(self, row_id: str) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            return fetch_one(conn, select(A).where(A.c.id == row_id))

    def _run_inline(self, view: RunView, task: Task, row: dict[str, Any]) -> None:
        try:
            full = self._fetch_full(task.row_id) or row
            actx = self._make_actx(task, full, view.run, resuming=task.resuming)
            out = task.d.impl.execute(actx)
        except BaseException as exc:  # noqa: BLE001
            self._conclude(task, error=exc)
            self._refresh(view, row)
            return
        self._conclude(task, output=out)
        self._refresh(view, row)

    def _refresh(self, view: RunView, row: dict[str, Any]) -> None:
        with self.db.connect() as conn:
            fresh = fetch_one(conn, select(*_LIGHT).where(A.c.id == row["id"]))
        if fresh:
            row.update(fresh)
        with self.db.connect() as conn:
            v = conn.execute(select(R.c.variables).where(R.c.id == view.run["id"])).scalar()
        if v is not None:
            view.variables = dict(v)

    def _run_phase(self, task: Task, phase: str) -> None:
        """Executed on the pool: one phase (exec | start | poll | resume) of an attempt."""
        impl = task.d.impl
        try:
            if self.dead or self._inflight.get(task.row_id) is not task:
                return
            full = self._fetch_full(task.row_id)
            if full is None or full["state"] != "running" or full["lease_token"] != task.token:
                return
            run = self.run_row(task.run_id)
            actx = self._make_actx(task, full, run, resuming=task.resuming or phase == "resume")
            with correlation.bind(run_id=run["id"], activity_id=task.row_id, workspace_id=run["workspace_id"]):
                if impl.mode == "blocking":
                    out: dict[str, Any] | None = impl.execute(actx)
                else:
                    if phase == "start":
                        impl.start(actx)
                        out = impl.poll(actx)
                    elif phase == "resume":
                        out = impl.resume(actx)
                    else:
                        out = impl.poll(actx)
                    if out is None:
                        task.started = True
                        task.next_poll = time.monotonic() + max(0.05, float(impl.poll_interval(actx)))
                        task.busy = False
                        return
            self._conclude(task, output=out)
        except BaseException as exc:  # noqa: BLE001
            self._conclude(task, error=exc)
        finally:
            task.busy = False
            self.wake()

    # ------------------------------------------------------------------------------------------------------------------------------ concluding attempts
    def _normalize_error(self, exc: BaseException) -> ActivityError | ActivityCancelled:
        if isinstance(exc, (ActivityError, ActivityCancelled)):
            return exc
        if isinstance(exc, ExpressionError):
            return ActivityError(exc.render(), category="expression", code=exc.code, retryable=False)
        if isinstance(exc, OlcfError):
            return ActivityError(exc.message, category=exc.code, code=exc.code, retryable=exc.status >= 500 or exc.status == 429,
                                 details={**exc.details, **({"remediation": exc.remediation} if exc.remediation else {})})
        log.exception("activity crashed")
        return ActivityError(f"internal error: {type(exc).__name__}: {redact_text(str(exc))[:300]}", category="internal", code="internal", retryable=False)

    def _conclude(self, task: Task, *, output: Any = None, error: BaseException | None = None, decided: bool = False) -> None:
        """Record the outcome of an attempt (compare-and-set on the lease token)."""
        if self.dead:
            return
        d = task.d
        now = self.now()
        effects = None
        err: ActivityError | ActivityCancelled | None = self._normalize_error(error) if error is not None else None
        if isinstance(err, LeaseLost):
            return
        if task.timed_out and not decided and not task.cancel_sent:
            # the timeout cancelled this attempt; what happened to it decides whether it may be replayed (see the module docstring)
            err = self._timeout_outcome(task, err, output)
            output = None
        if err is None:
            out = output if isinstance(output, dict) else ({} if output is None else {"value": output})
            out = dict(out)
            effects = out.pop("_effects", None)
            try:
                size = len(json.dumps(out, default=str))
            except (TypeError, ValueError) as exc:
                err = ActivityError(f"activity output is not JSON serialisable: {exc}", category="internal", retryable=False)
            else:
                if size > MAX_OUTPUT_BYTES:
                    err = ActivityError(f"activity output is {size // 1024} KiB; the limit is {MAX_OUTPUT_BYTES // 1024 // 1024} MiB (write large results to storage and pass a path)",
                                        category="limit", code="output_too_large", retryable=False)
        with self.db.begin() as conn:
            cur = conn.execute(select(A.c.attempts, A.c.started_at, A.c.attempt, A.c.state_data, A.c.max_attempts).where(A.c.id == task.row_id, A.c.lease_token == task.token,
                                                                                                        A.c.state == "running").with_for_update()).first()
            if cur is None:
                self._inflight.pop(task.row_id, None)
                return
            attempts = list(cur[0] or [])
            rec = {"attempt": cur[2], "started_at": task.started_iso, "finished_at": to_iso(now)}
            dur = int((now - from_iso(cur[1])).total_seconds() * 1000) if cur[1] else None
            vals: dict[str, Any] = {"lease_token": None, "updated_at": to_iso(now), "duration_ms": dur}
            retry_at = None
            if err is None:
                rec["state"] = "succeeded"
                vals.update(state="succeeded", finished_at=to_iso(now), output=out, error=null())
                if effects:
                    sd = dict(cur[3] or {})
                    sd["effects"] = effects
                    vals["state_data"] = sd
                    run_vars = conn.execute(select(R.c.variables).where(R.c.id == task.run_id).with_for_update()).scalar() or {}
                    conn.execute(update(R).where(R.c.id == task.run_id).values(variables={**run_vars, **(effects.get("variables") or {})}, updated_at=to_iso(now)))
                final = "succeeded"
            elif isinstance(err, ActivityCancelled):
                rec.update(state="cancelled")
                vals.update(state="cancelled", finished_at=to_iso(now), error={"code": "cancelled", "message": "cancelled"})
                final = "cancelled"
            else:
                rec.update(state="failed", error=redact_obj(err.to_dict()))
                if err.retryable and cur[2] < cur[4]:
                    back = min(float(d.policy["max_backoff"]), float(d.policy["backoff"]) * (float(d.policy["multiplier"]) ** (cur[2] - 1)))
                    retry_at = to_iso(now + timedelta(seconds=back))
                    vals.update(state="retry_wait", next_attempt_at=retry_at, error=redact_obj(err.to_dict()))
                    final = "retry_wait"
                else:
                    vals.update(state="failed", finished_at=to_iso(now), error=redact_obj(err.to_dict()))
                    final = "failed"
            vals["attempts"] = [*attempts, rec]
            conn.execute(update(A).where(A.c.id == task.row_id).values(**vals))
        self._inflight.pop(task.row_id, None)
        if final in ("succeeded", "failed", "cancelled"):
            with self.db.connect() as conn:
                run = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == task.run_id))
            if run:
                self._after_activity(run, {"type": d.type, "name": d.name, "duration_ms": dur, "error": vals.get("error"), "attempt": cur[2]}, final)
        elif final == "retry_wait" and str((vals.get("error") or {}).get("category", "")).endswith("_uncertain"):
            with self.db.connect() as conn:
                run = fetch_one(conn, select(*_RUN_COLS).where(R.c.id == task.run_id))
            if run:
                self._announce_uncertain(run, {"type": d.type, "name": d.name, "error": vals.get("error"), "attempt": cur[2]}, retried=True)
        self.wake()

    def _is_idempotent(self, task: Task) -> bool:
        """May this attempt be executed again even though it might already have done its work? (``policy.idempotent`` or the activity's own knowledge.)"""
        try:
            return bool(task.d.policy.get("idempotent")) or bool(task.d.impl.idempotent_for(task.props))
        except Exception:  # noqa: BLE001 - a plugin's idempotent_for must not break the engine; assume the worst
            return False

    def _grace_for(self, d: ActDef) -> float:
        g = d.policy.get("cancel_grace")
        return float(g) if g is not None else float(self.timeout_cancel_grace)

    def _time_out(self, view: RunView, row: dict[str, Any], task: Task) -> None:
        """The attempt exceeded its timeout: signal cancellation and wait for it to *stop*; it is not concluded (and so not retried) here.

        Concluding first - as the engine used to - let the retry start while the original attempt could still commit its effects.
        """
        task.timed_out = True  # before the cancel flag: the worker thread reads it as soon as it observes the flag
        now = time.monotonic()
        task.timeout_signalled = now
        task.timeout_deadline = now + self._grace_for(task.d)
        task.cancel.set()
        with self.db.begin() as conn:
            conn.execute(update(A).where(A.c.id == row["id"], A.c.lease_token == task.token).values(cancel_requested=True, updated_at=self.now_iso()))
        row["cancel_requested"] = True
        log.warning("activity %s (%s) exceeded its timeout; cancelling and waiting up to %.0fs for it to stop", row["id"], task.d.type, self._grace_for(task.d))
        if task.d.impl.mode != "inline":
            task.next_cancel_hook = now + 2.0
            self._best_effort_cancel(task, view.run)
        self.wake()

    def _drive_timed_out(self, view: RunView, row: dict[str, Any], task: Task) -> None:
        """Cancelling phase of a timed-out attempt: keep asking it to stop, poll async work until it is terminal, give up (uncertain) when the grace ends."""
        now = time.monotonic()
        if task.d.impl.mode == "async" and not task.busy and task.started and now >= task.next_poll:
            task.busy = True
            self._pool.submit(self._run_phase, task, "poll")
        if now >= task.next_cancel_hook and task.d.impl.mode != "inline":
            task.next_cancel_hook = now + 2.0
            self._best_effort_cancel(task, view.run)  # idempotent hook: also reaches a job that was submitted after the deadline passed
        if task.timeout_deadline is not None and now > task.timeout_deadline:
            tmo = task.d.policy.get("timeout")
            idem = self._is_idempotent(task)
            err = ActivityError(
                f"activity '{task.d.name}' exceeded its timeout of {tmo} s and did not stop within the {self._grace_for(task.d):g} s cancellation grace period; "
                "whether it completed its work is unknown", category="timeout_uncertain", code="timeout_uncertain", retryable=idem,
                details=self._uncertain_details(task, still_running=True, idempotent=idem))
            log.error("activity %s (%s) did not stop after its timeout: outcome unknown (retry %s)", row["id"], task.d.type, "allowed (idempotent)" if idem else "refused")
            self._conclude(task, error=err, decided=True)
            self._refresh(view, row)

    def _uncertain_details(self, task: Task, *, still_running: bool, idempotent: bool) -> dict[str, Any]:
        return {"outcome": "unknown", "attemptStillRunning": still_running, "timedOut": True, "idempotent": idempotent,
                "operatorAction": ("The attempt may have completed its work. Check the target system, then decide: "
                                   "re-run from failure (POST /v1/pipeline-runs/{run_id}/resume) if repeating it is safe, or leave the run failed. "
                                   "Mark the activity policy.idempotent=true (or send an Idempotency-Key) to let retries repeat it automatically.")}

    def _timeout_outcome(self, task: Task, err: ActivityError | ActivityCancelled | None, output: Any) -> ActivityError:
        """Translate what a cancelled attempt did into the recorded outcome (it has stopped when this is called)."""
        d = task.d
        tmo = d.policy.get("timeout")
        idem = self._is_idempotent(task)
        waited = round(time.monotonic() - task.timeout_signalled, 2) if task.timeout_signalled is not None else None
        base = f"activity '{d.name}' timed out: it exceeded its timeout of {tmo} s"
        details: dict[str, Any] = {"timedOut": True, "stoppedAfterSeconds": waited}
        if err is None:  # ran to completion in spite of the cancellation request: its effects exist, so replaying is only for idempotent work
            return ActivityError(f"{base} and then completed; its result is discarded because it ran past its deadline", category="timeout", code="timeout", retryable=idem,
                                 details={**details, "lateOutcome": "succeeded", "idempotent": idem})
        if isinstance(err, ActivityCancelled):
            if err.uncertain:
                return ActivityError(f"{base}; {err.message}", category="timeout_uncertain", code="timeout_uncertain", retryable=idem,
                                     details={**err.details, **details, **self._uncertain_details(task, still_running=False, idempotent=idem)})
            return ActivityError(f"{base}; it stopped when cancelled", category="timeout", code="timeout", retryable=True, details=details)
        if err.category.endswith("_uncertain"):  # the activity itself could not tell whether its work was accepted
            return ActivityError(err.message, category=err.category, code=err.code, retryable=err.retryable and idem,
                                 details={**err.details, **details, **self._uncertain_details(task, still_running=False, idempotent=idem)})
        # a definite error that arrived after the deadline: keep it, but it is only replayed when repeating is safe
        return ActivityError(err.message, category=err.category, code=err.code, retryable=err.retryable and idem, details={**err.details, **details, "afterTimeout": True})

    def _best_effort_cancel(self, task: Task, run: dict[str, Any]) -> None:
        def go() -> None:
            try:
                full = self._fetch_full(task.row_id)
                if full is None:
                    return
                actx = self._make_actx(task, full, run)
                task.d.impl.cancel(actx)
            except Exception:  # noqa: BLE001
                log.debug("activity cancel hook failed", exc_info=True)

        self._pool.submit(go)

    def _cancel_task(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any], task: Task) -> None:
        if not task.cancel_sent:
            task.cancel_sent = True
            task.cancel.set()
            task.cancel_deadline = time.monotonic() + self.cancel_grace
            with self.db.begin() as conn:
                conn.execute(update(A).where(A.c.id == row["id"]).values(cancel_requested=True))
            if d.impl.mode == "async":
                self._best_effort_cancel(task, view.run)
            return
        if d.impl.mode == "async" and not task.busy and task.started and time.monotonic() >= task.next_poll:
            task.busy = True
            self._pool.submit(self._run_phase, task, "poll")
        if task.cancel_deadline is not None and time.monotonic() > task.cancel_deadline:
            self._conclude(task, error=ActivityCancelled())
            self._refresh(view, row)

    # ------------------------------------------------------------------------------------------------------------------------------ adoption
    def _adopt(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any]) -> None:
        """Take over a ``running`` attempt whose lease belongs to a dispatcher that no longer exists (crash / failover)."""
        ectx = self._ectx(view, scope)
        try:
            props = evaluate_value(d.props, ectx)
        except Exception as exc:  # noqa: BLE001
            self._finish_unleased(view, row, "failed", error={"code": "interrupted", "category": "interrupted", "message": f"cannot re-evaluate the interrupted activity: {exc}", "retryable": False})
            return
        token = secrets.token_hex(16)
        with self.db.begin() as conn:
            n = conn.execute(update(A).where(A.c.id == row["id"], A.c.state == "running", A.c.lease_token == row["lease_token"]).values(lease_token=token, updated_at=self.now_iso())).rowcount
        if not n:
            return
        row["lease_token"] = token
        snapshot = EvalContext(parameters=ectx.parameters, variables=dict(view.variables), pipeline=ectx.pipeline, trigger=ectx.trigger, activity=ectx.activity, item=ectx.item,
                               item_index=ectx.item_index, has_item=ectx.has_item, now=ectx.now, workspace=ectx.workspace)
        task = Task(row_id=row["id"], token=token, run_id=view.run["id"], d=d, props=props, attempt=int(row["attempt"]), expr=ectx if d.impl.mode == "inline" else snapshot,
                    resuming=True, epoch=self._epoch)
        self._inflight[row["id"]] = task
        mode = d.impl.mode
        log.info("adopting interrupted activity %s (%s, attempt %s)", row["id"], d.type, row["attempt"])
        if mode == "async":
            task.busy = True
            self._pool.submit(self._run_phase, task, "resume")
        elif mode == "inline" or (mode == "blocking" and (d.impl.idempotent_for(props) or d.policy.get("idempotent"))):
            if mode == "inline":
                self._run_inline(view, task, row)
            else:
                task.busy = True
                self._pool.submit(self._run_phase, task, "exec")
        else:
            self._conclude(task, error=ActivityError(
                f"the control plane stopped while '{d.name}' ({d.type}) was running; it is not repeated automatically because it may not be safe to run twice "
                "(set policy.idempotent=true to allow it, or re-run the pipeline from failure)", category="interrupted", code="interrupted", retryable=False))
            self._refresh(view, row)

    # ------------------------------------------------------------------------------------------------------------------------------ containers
    def _iter_scope(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any], sd: dict[str, Any], i: int) -> Scope:
        impl: Container = d.impl  # type: ignore[assignment]
        raw_defs = impl.scope_defs(d.props, sd)
        return Scope(defs=view.rdef.wrap_all(raw_defs), parent_id=row["id"], iteration=i, chain=((row["id"], i), *scope.chain), over={**scope.over, **impl.iteration_context(sd, i)},
                     path=f"{self._row_path(scope, d.name)}/{i}/", stable=bool(sd.get("stable", scope.stable)))

    def _drive_container(self, view: RunView, scope: Scope, d: ActDef, row: dict[str, Any]) -> None:
        impl: Container = d.impl  # type: ignore[assignment]
        run = view.run
        if row["state"] in ("ready", "retry_wait"):
            if view.cancelling:
                self._finish_unleased(view, row, "cancelled", error={"code": "cancelled", "message": "run cancelled"})
                return
            ectx = self._ectx(view, scope)
            try:
                props = impl.evaluate_props(d.props, lambda v: evaluate_value(v, ectx))
                actx = ActivityContext(ctx=self.ctx, principal=self.principal_for(run), run=run, row=row, definition=d.raw, props=props, state={}, attempt=1, expr=ectx, engine=self,
                                       pipeline_def=view.rdef.definition, now=self.now)
                sd = impl.begin(actx)
            except ExpressionError as exc:
                self._finish_unleased(view, row, "failed", error={"code": exc.code, "category": "expression", "message": f"expression error in '{d.name}': {exc.render()}", "retryable": False})
                return
            except ActivityError as exc:
                self._finish_unleased(view, row, "failed", error=exc.to_dict())
                return
            except OlcfError as exc:
                self._finish_unleased(view, row, "failed", error={"code": exc.code, "category": exc.code, "message": exc.message})
                return
            sd = dict(sd)
            # inner activities may only be re-used from a previous run when this container's own inputs are unchanged
            sd["stable"] = bool(scope.stable) and all((view.get(scope, dep["activity"]) or {}).get("reused_from") is not None for dep in d.deps)
            now = self.now()
            tmo = self._timeout_for(d, props)
            vals = {"state": "running", "attempt": 1, "started_at": to_iso(now), "state_data": sd, "input": self._make_input(d, props), "updated_at": to_iso(now),
                    "timeout_at": to_iso(now + timedelta(seconds=float(tmo))) if tmo else None}
            with self.db.begin() as conn:
                n = conn.execute(update(A).where(A.c.id == row["id"], A.c.state.in_(("ready", "retry_wait"))).values(**vals)).rowcount
            if not n:
                return
            row.update({k: v for k, v in vals.items() if k in row})
            view._sd[row["id"]] = sd
        sd = view.state_data(row["id"])
        if row.get("timeout_at") and row["timeout_at"] < self.now_iso() and not view.cancelling:
            self._cancel_subtree(view, row)
            self._finish_unleased(view, row, "failed", error={"code": "timeout", "category": "timeout", "message": f"activity '{d.name}' exceeded its timeout", "retryable": False})
            return
        n_it = impl.iterations(sd)
        par = max(1, impl.parallelism(sd))
        present = sorted({r["iteration"] for r in view.children.get(row["id"], []) if r["iteration"] is not None})
        status: dict[int, str] = {}
        for i in present:
            status[i] = self._walk(view, self._iter_scope(view, scope, d, row, sd, i))
        active = [i for i, s in status.items() if s == "running"]
        nxt = (present[-1] + 1) if present else 0
        finished = False
        failed_any = any(s in ("failed", "cancelled") for s in status.values())
        stop_new = view.cancelling or (failed_any and not sd.get("continue", True))
        if n_it is not None:
            while not stop_new and nxt < n_it and len(active) < par:
                s = self._walk(view, self._iter_scope(view, scope, d, row, sd, nxt))
                status[nxt] = s
                if s == "running":
                    active.append(nxt)
                if s in ("failed", "cancelled"):
                    failed_any = True
                    stop_new = stop_new or not sd.get("continue", True)
                nxt += 1
            finished = not active and (nxt >= n_it or stop_new)
        else:  # open ended (until)
            if not present and not stop_new:
                s = self._walk(view, self._iter_scope(view, scope, d, row, sd, 0))
                status[0] = s
                nxt = 1
            elif not active:
                last = nxt - 1
                if view.cancelling or last < 0:
                    finished = True
                elif int(sd.get("checked", -1)) < last:
                    ok = status.get(last) == "succeeded"
                    sc = self._iter_scope(view, scope, d, row, sd, last)
                    actx = ActivityContext(ctx=self.ctx, principal=self.principal_for(run), run=run, row=row, definition=d.raw, props={}, state=sd, attempt=1,
                                           expr=self._ectx(view, sc), engine=self, pipeline_def=view.rdef.definition, now=self.now)
                    try:
                        again = impl.after_iteration(actx, sd, last, ok)
                    except ActivityError as exc:
                        self._cancel_subtree(view, row)
                        self._finish_unleased(view, row, "failed", error=exc.to_dict())
                        return
                    sd["checked"] = last
                    with self.db.begin() as conn:
                        conn.execute(update(A).where(A.c.id == row["id"]).values(state_data=sd, updated_at=self.now_iso()))
                    if again:
                        s = self._walk(view, self._iter_scope(view, scope, d, row, sd, nxt))
                        status[nxt] = s
                        nxt += 1
                    else:
                        finished = True
                else:
                    finished = True
        if not finished:
            return
        results = []
        for i in sorted(status):
            s = status[i]
            outs: dict[str, Any] = {}
            col = sd.get("collect")
            if col and s == "succeeded":
                inner = view.rows.get((row["id"], i, col))
                if inner is not None and inner["state"] == "succeeded":
                    outs[col] = view.output(inner["id"])
            results.append({"index": i, "state": "succeeded" if s == "succeeded" else "failed", "outputs": outs})
        if view.cancelling:
            self._finish_unleased(view, row, "cancelled", error={"code": "cancelled", "message": "run cancelled"})
            return
        if any(s in ("failed", "cancelled") for s in status.values()):
            bad = [i for i, s in sorted(status.items()) if s != "succeeded"]
            self._finish_unleased(view, row, "failed", error={"code": "inner_failed", "category": "inner_failed", "retryable": False,
                                                              "message": f"{len(bad)} of {len(status)} iteration(s) of '{d.name}' failed (iterations {bad[:10]}{'...' if len(bad) > 10 else ''})"},
                                  output=impl.finalize(sd, results))
            return
        try:
            out = impl.finalize(sd, results)
        except Exception as exc:  # noqa: BLE001
            self._finish_unleased(view, row, "failed", error={"code": "internal", "message": f"finalize failed: {exc}"})
            return
        self._finish_unleased(view, row, "succeeded", output=out)

    def _cancel_subtree(self, view: RunView, row: dict[str, Any]) -> None:
        stack = [row["id"]]
        while stack:
            pid = stack.pop()
            for ch in view.children.get(pid, []):
                if ch["state"] in ACT_TERMINAL:
                    continue
                t = self._inflight.get(ch["id"])
                if t is not None:
                    t.cancel.set()
                    if t.d.impl.mode == "async":
                        self._best_effort_cancel(t, view.run)
                    self._conclude(t, error=ActivityCancelled())
                    self._refresh(view, ch)
                else:
                    self._finish_unleased(view, ch, "cancelled", error={"code": "cancelled", "message": "parent activity ended"})
                stack.append(ch["id"])

    # ------------------------------------------------------------------------------------------------------------------------------ finalisation
    def _finalize(self, view: RunView, status: str) -> None:
        run = view.run
        if run["state"] in RUN_TERMINAL:
            return
        with self.db.connect() as conn:
            failures = fetch_all(conn, select(A.c.id, A.c.name, A.c.type, A.c.error, A.c.parent_id, A.c.iteration).where(A.c.run_id == run["id"], A.c.state == "failed")
                                 .order_by(A.c.finished_at).limit(30))
        handled = {f["id"] for f in failures if self._is_handled(view, f)}
        failures = [f for f in failures if f["id"] not in handled] + [f for f in failures if f["id"] in handled]   # unhandled failures first: they are why the run failed
        failed = [{"activity": view.paths.get(f["id"], f["name"]), "type": f["type"], "error": f["error"], **({"handled": True} if f["id"] in handled else {})} for f in failures]
        reason = run.get("cancel_reason") or ""
        if view.cancelling or status == "cancelled":
            plain = reason.removeprefix("failfast: ")
            if reason.startswith("failfast:") or plain.startswith("run exceeded"):
                timed_out = plain.startswith("run exceeded")
                first = None if timed_out else (failed[0] if failed else None)
                msg = plain if not first else f"activity '{first['activity']}' failed: {(first['error'] or {}).get('message', '')}"
                code = "run_timeout" if timed_out else "activity_failed"
                self._set_run_state(run, "failed", msg, finished=True, error={"code": code, "message": msg, "failed_activities": failed})
            else:
                self._set_run_state(run, "cancelled", run.get("state_reason") or "cancelled", finished=True)
            return
        if status == "failed":
            first = failed[0] if failed else None
            msg = f"activity '{first['activity']}' failed: {(first['error'] or {}).get('message', '')}" if first else "the pipeline failed"
            self._set_run_state(run, "failed", msg[:500], finished=True, error={"code": "activity_failed", "message": msg[:1000], "failed_activities": failed})
            return
        outputs: dict[str, Any] = {}
        defn_outputs = view.rdef.definition.get("outputs") or {}
        if defn_outputs:
            ectx = self._ectx(view, Scope(defs=view.rdef.root, parent_id=None, iteration=None, chain=((None, None),), over={}, path=""))
            try:
                outputs = evaluate_value(defn_outputs, ectx)
            except (ExpressionError, OlcfError) as exc:
                detail = exc.render() if isinstance(exc, ExpressionError) else str(exc)
                msg = f"pipeline outputs could not be evaluated: {detail}"
                self._set_run_state(run, "failed", msg[:500], finished=True, error={"code": "outputs_failed", "message": msg[:1000]})
                return
        self._set_run_state(run, "succeeded", None, finished=True, outputs=outputs)
        self._record_lineage(run)

    @staticmethod
    def _timeout_for(d: ActDef, props: dict[str, Any]) -> float | None:
        """Activity timeout in seconds: ``policy.timeoutSeconds`` when set, else what the activity type derives from its evaluated properties (or its default)."""
        if d.policy.get("timeout_explicit"):
            return d.policy.get("timeout")
        try:
            return d.impl.timeout_for(props)
        except Exception:  # noqa: BLE001
            return d.policy.get("timeout")

    def _is_handled(self, view: RunView, failed: dict[str, Any]) -> bool:
        """Did a sibling that waits for this activity's ``failed`` / ``completed`` outcome actually run (i.e. is the failure handled)?"""
        names: list[str] = []
        r: dict[str, Any] | None = failed
        while r is not None:
            names.append(r["name"])
            r = view.by_id.get(r["parent_id"]) if r.get("parent_id") else None
        sid = "/".join(reversed(names))
        prefix = sid.rsplit("/", 1)[0] + "/" if "/" in sid else ""
        for key, d in view.rdef.index().items():
            if not key.startswith(prefix) or "/" in key[len(prefix):]:
                continue
            if any(dep["activity"] == failed["name"] and any(c in ("failed", "completed") for c in dep["conditions"]) for dep in d.deps):
                row = view.rows.get((failed.get("parent_id"), failed.get("iteration"), d.name))
                if row is not None and row["state"] != "skipped":
                    return True
        return False

    def _record_lineage(self, run: dict[str, Any]) -> None:
        cat = self.ctx.maybe_service("catalog")
        if cat is None:
            return
        try:
            with self.db.connect() as conn:
                rows = fetch_all(conn, select(A.c.type, A.c.state_data).where(A.c.run_id == run["id"], A.c.state == "succeeded", A.c.type.in_(("notebook", "spark_job"))))
            ids = sorted({(r["state_data"] or {}).get("item_id") for r in rows if (r["state_data"] or {}).get("item_id")})
            if not ids:
                return
            cat.record_lineage(None, workspace_id=run["workspace_id"], sources=[{"kind": "item", "item_id": run["pipeline_id"]}],
                               targets=[{"kind": "notebook", "item_id": i} for i in ids], via={"kind": "pipeline", "name": run["pipeline_name"], "ref": run["id"]}, authorize=False)
        except Exception:  # noqa: BLE001 - lineage is best effort
            log.debug("lineage recording failed", exc_info=True)

    # ------------------------------------------------------------------------------------------------------------------------------ child runs (used by child_pipeline)
    def start_child(self, actx: ActivityContext, ref: str, parameters: dict[str, Any]) -> dict[str, Any]:
        """Create (or find, idempotently per attempt) the child run of a ``child_pipeline`` activity."""
        run = actx.run
        items = self.ctx.service("items")
        item = items.resolve(run["workspace_id"], ref, "pipeline")
        actx.principal.require("pipeline.run", workspace_id=item["workspace_id"], item_id=item["id"])
        depth = int(run.get("depth") or 0) + 1
        if depth > MAX_CHILD_DEPTH:
            raise BadRequest(f"pipelines can be nested at most {MAX_CHILD_DEPTH} levels deep")
        cur: dict[str, Any] | None = run
        seen = 0
        while cur is not None and seen < 64:
            if cur["pipeline_id"] == item["id"]:
                raise BadRequest(f"recursive pipeline call: '{item['name']}' is already running higher up in this call chain", remediation="a pipeline cannot (indirectly) execute itself")
            pid = cur.get("parent_run_id")
            cur = self.run_row(pid) if pid else None
            seen += 1
        return self.create_run(actx.principal, item, parameters, {"type": "child", "parentRunId": run["id"], "parentActivity": actx.name}, idempotency_key=f"child:{actx.activity_run_id}:{actx.attempt}",
                               parent_run=run, parent_activity_id=actx.activity_run_id, trigger_type="child")

    # ------------------------------------------------------------------------------------------------------------------------------ resume
    def resume_run(self, principal: Any, run_id: str, *, rerun: list[str] | None = None, parameters: dict[str, Any] | None = None, latest_definition: bool = False,
                   reuse: bool = True, idempotency_key: str | None = None) -> dict[str, Any]:
        """Start a new run that re-uses the successful results of ``run_id`` (a failed or cancelled run) and re-executes the rest."""
        old = self.run_row(run_id)
        if old["state"] not in ("failed", "cancelled", "succeeded"):
            raise Conflict(f"run {run_id} is {old['state']}; only finished runs can be resumed or re-run")
        items = self.ctx.service("items")
        try:
            item = items.get(old["pipeline_id"])
        except NotFound:
            raise NotFound("the pipeline of this run no longer exists") from None
        with self.db.connect() as conn:
            snap = conn.execute(select(R.c.definition).where(R.c.id == run_id)).scalar()
        if latest_definition:
            defn = self.definition_of(item)
        else:
            defn = snap
        params = dict(old["parameters"] or {})
        changed = False
        if parameters:
            for k, v in parameters.items():
                if params.get(k) != v:
                    changed = True
            params.update(parameters)
        if rerun:
            known = {a for a in M.activity_defs_index(defn, self.registry) for a in a}
            unknown = [n for n in rerun if n not in known]
            if unknown:
                raise ValidationFailed(f"unknown activity name(s) to re-run: {unknown}", details={"known": sorted(known)})
        opts = {"rerun": list(rerun or []), "reuse": bool(reuse) and not changed, "latest_definition": latest_definition, "parameters_changed": changed}
        trig = dict(old.get("trigger") or {})
        trig.update({"resumedFrom": old["id"]})
        return self.create_run(principal, item, params, trig, idempotency_key=idempotency_key, resumed_from=old, resume_options=opts, definition=defn, trigger_type=old["trigger_type"])


def _contains_names(raw: dict[str, Any], names: set[str], registry: ActivityRegistry) -> bool:
    impl = registry.get(raw.get("type", ""))
    if not isinstance(impl, Container):
        return False
    for _k, inner in impl.scopes(raw.get("typeProperties") or {}):
        for a in inner or []:
            if a.get("name") in names or _contains_names(a, names, registry):
                return True
    return False


def _dur(run: dict[str, Any]) -> float | None:
    try:
        if run.get("started_at") and run.get("finished_at"):
            return round((from_iso(run["finished_at"]) - from_iso(run["started_at"])).total_seconds(), 3)
    except Exception:  # noqa: BLE001
        return None
    return None


__all__ = ["LeaseLost", "PipelineEngine", "RunDef", "RunView", "Task", "lid"]
