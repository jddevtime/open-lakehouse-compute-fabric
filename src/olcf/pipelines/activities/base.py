# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Activity plugin interface and registry (public, versioned: ``ACTIVITY_API_VERSION``).

An *activity type* is a class deriving from :class:`Activity`. Built-in types live in this package; third parties register more through
the ``olcf.pipeline_activities`` entry-point group (the entry point may be an ``Activity`` subclass or a callable returning an iterable of
them)::

    # pyproject.toml of a plugin
    [project.entry-points."olcf.pipeline_activities"]
    my_types = "my_pkg.olcf_activities:ACTIVITIES"

Execution modes
---------------
``blocking``  ``execute(actx)`` runs on a worker thread of the engine and returns the output dict (use for copy, sql, http...).
``async``     ``start(actx)`` kicks something off (and persists handles with ``actx.save_state``), the engine then calls ``poll(actx)`` until it
              returns a result; nothing is held while waiting, so it is naturally durable (notebook / spark_job / script jobs, wait, child
              pipelines, webhook callbacks).
``inline``    ``execute(actx)`` is quick and runs on the dispatcher thread (set_variable, fail).
``container`` the engine expands inner activity scopes (if / switch / for_each / until); see :class:`Container`.

Crash semantics
---------------
After a crash/failover the engine calls ``resume`` (async) or - for blocking activities - re-runs ``execute`` only when the type declares
``resumable = True`` (it must then be safe to run twice, e.g. copy with its manifest) or the activity sets ``policy.idempotent``; otherwise the
activity is failed as *interrupted* and never silently executed a second time.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from olcf.core.errors import OlcfError

log = logging.getLogger("olcf.pipelines.activities")

ACTIVITY_API_VERSION = "1"
ENTRY_POINT_GROUP = "olcf.pipeline_activities"


class ActivityError(Exception):
    """Failure of one activity attempt. ``retryable=False`` bypasses the retry policy (configuration errors, ``fail`` activity...)."""

    def __init__(self, message: str, *, category: str = "activity", code: str | None = None, retryable: bool = True, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message, self.category, self.code, self.retryable, self.details = message, category, code or category, retryable, details or {}

    def to_dict(self) -> dict[str, Any]:
        d = {"category": self.category, "code": self.code, "message": self.message, "retryable": self.retryable}
        if self.details:
            d["details"] = self.details
        return d


class ActivityCancelled(Exception):
    """Raised by activities when they observe cancellation and have stopped.

    ``uncertain=True`` says that the activity had already handed work to an external system (an HTTP request that was sent, a statement that was submitted) when it stopped,
    so it cannot tell whether that work was accepted.  After a timeout the engine then records ``timeout_uncertain`` and replays the attempt only for idempotent work.
    """

    def __init__(self, message: str = "cancelled", *, uncertain: bool = False, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message, self.uncertain, self.details = message, uncertain, details or {}


@dataclass
class ValidationIssue:
    """One finding of definition validation (``path`` like ``activities[2].typeProperties.notebook``)."""

    path: str
    message: str
    code: str = "invalid"
    severity: str = "error"  # error | warning

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "message": self.message, "code": self.code, "severity": self.severity}


@dataclass
class ActivityContext:
    """Everything an activity implementation may use. Created per execution attempt by the engine."""

    ctx: Any                       # AppContext
    principal: Any                 # Principal the run executes as
    run: dict[str, Any]            # pipeline_runs row (definition omitted)
    row: dict[str, Any]            # activity_runs row
    definition: dict[str, Any]     # the activity definition (name, type, typeProperties, policy ...)
    props: dict[str, Any]          # typeProperties with expressions evaluated
    state: dict[str, Any]          # persistent activity state (survives crashes)
    attempt: int = 1
    resuming: bool = False
    expr: Any = None               # EvalContext (for activities that evaluate expressions later, e.g. containers)
    engine: Any = None
    pipeline_def: dict[str, Any] = field(default_factory=dict)
    signal: Any = None             # external signal delivered to the activity (webhook callback body), read fresh on every poll
    now: Callable[[], Any] = field(default=lambda: __import__("datetime").datetime.now(__import__("datetime").UTC))
    _save: Callable[[dict[str, Any]], None] | None = None
    _link_job: Callable[[str], None] | None = None
    _cancel: threading.Event = field(default_factory=threading.Event)
    _logs: list[str] = field(default_factory=list)

    # -- identity helpers
    @property
    def workspace_id(self) -> str:
        return self.run["workspace_id"]

    @property
    def run_id(self) -> str:
        return self.run["id"]

    @property
    def activity_run_id(self) -> str:
        return self.row["id"]

    @property
    def name(self) -> str:
        return self.definition["name"]

    @property
    def log_prefix(self) -> str:
        return f"[{self.run_id}/{self.name}#{self.attempt}]"

    # -- state & cancellation
    def save_state(self, **kv: Any) -> None:
        """Merge ``kv`` into the persisted activity state (durable before returning)."""
        self.state.update(kv)
        if self._save is not None:
            self._save(dict(self.state))

    def link_job(self, job_id: str) -> None:
        """Record that this activity run owns ``job_id`` (shown in the run graph as a logs link)."""
        if self._link_job is not None:
            self._link_job(job_id)

    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def check_cancelled(self) -> None:
        if self._cancel.is_set():
            raise ActivityCancelled()

    def sleep(self, seconds: float) -> bool:
        """Cancellation-aware sleep; returns True when cancelled."""
        return self._cancel.wait(seconds)

    def on_cancel(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Call ``callback`` (once, from a helper thread) as soon as cancellation is requested - a timeout or a run cancel.

        Use it to abort a blocking call that cannot poll :meth:`is_cancelled` (a socket read, a driver call).  Returns a function that disarms the
        watcher; call it when the blocking call is over.  ``callback`` must not raise.
        """
        done = threading.Event()

        def watch() -> None:
            while not done.is_set():
                if self._cancel.wait(0.05):
                    if not done.is_set():
                        try:
                            callback()
                        except Exception:  # noqa: BLE001 - an abort hook must never take the watcher down
                            log.debug("on_cancel callback failed", exc_info=True)
                    return

        threading.Thread(target=watch, name="olcf-activity-cancel-watch", daemon=True).start()
        return done.set

    def log(self, msg: str) -> None:
        log.info("%s %s", self.log_prefix, msg)

    def service(self, name: str) -> Any:
        return self.ctx.service(name)


class Activity:
    """Base class of every activity type. Subclasses set the class attributes and implement the hooks of their ``mode``."""

    type: ClassVar[str] = ""
    api_version: ClassVar[str] = ACTIVITY_API_VERSION
    version: ClassVar[str] = "1.0"
    title: ClassVar[str] = ""
    description: ClassVar[str] = ""
    mode: ClassVar[str] = "blocking"        # blocking | async | inline | container
    reusable: ClassVar[bool] = True         # successful result may be reused when re-running from failure
    resumable: ClassVar[bool] = False       # blocking: safe to run again after a crash
    default_timeout_seconds: ClassVar[int | None] = None
    properties_schema: ClassVar[dict[str, Any]] = {"type": "object"}
    #: typeProperties paths (dotted) whose values are never stored/shown (secret references are fine, secret values must not appear here anyway)
    sensitive_props: ClassVar[tuple[str, ...]] = ()

    # ---- static validation -----------------------------------------------------------------------------------------------
    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        """Semantic checks beyond the JSON schema (called with the raw, unevaluated ``typeProperties``)."""
        return []

    # ---- runtime ---------------------------------------------------------------------------------------------------------
    def execute(self, actx: ActivityContext) -> dict[str, Any]:  # blocking / inline
        raise NotImplementedError(f"activity type '{self.type}' does not implement execute()")

    def start(self, actx: ActivityContext) -> None:  # async
        raise NotImplementedError(f"activity type '{self.type}' does not implement start()")

    def poll(self, actx: ActivityContext) -> dict[str, Any] | None:  # async; None = still running
        raise NotImplementedError(f"activity type '{self.type}' does not implement poll()")

    def resume(self, actx: ActivityContext) -> dict[str, Any] | None:  # async, after crash/failover
        """Default: ask poll() again - state persisted by start() is enough for every built-in async activity."""
        if not actx.state:
            self.start(actx)
        return self.poll(actx)

    def cancel(self, actx: ActivityContext) -> None:
        """Best-effort cancellation of whatever the activity started (jobs, child runs). Must not raise."""

    def timeout_for(self, props: dict[str, Any]) -> float | None:
        """Timeout (seconds) of an attempt when ``policy.timeoutSeconds`` is not set; receives the *evaluated* typeProperties. Default: ``default_timeout_seconds``."""
        return self.default_timeout_seconds

    def idempotent_for(self, props: dict[str, Any]) -> bool:
        """May this activity (with these *evaluated* properties) be executed again after a crash? Default: ``resumable``."""
        return self.resumable

    def poll_interval(self, actx: ActivityContext) -> float:
        return 1.0

    # ---- documentation ---------------------------------------------------------------------------------------------------
    def public_state(self, state: dict[str, Any]) -> dict[str, Any]:
        """Parts of the persisted activity state that holders of ``pipeline.run`` may see while the activity is active (default: none).

        ``webhook_wait`` uses it to show the callback URL. The returned values are merged into the activity's API representation under their own keys.
        """
        return {}

    def describe(self) -> dict[str, Any]:
        return {"type": self.type, "version": self.version, "api_version": self.api_version, "title": self.title or self.type, "description": self.description,
                "mode": self.mode, "reusable": self.reusable, "resumable": self.resumable, "properties_schema": self.properties_schema}


class Container(Activity):
    """Activity that owns nested activity scopes. The engine creates the inner rows; subclasses decide *which* and *how many*."""

    mode: ClassVar[str] = "container"
    reusable: ClassVar[bool] = True

    def scopes(self, props: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
        """Static scopes ``[(property path, activities)]`` of the raw typeProperties (validation + graph)."""
        return []

    #: typeProperties keys that hold nested activity lists (never expression-evaluated by the engine)
    scope_keys: ClassVar[tuple[str, ...]] = ()
    #: typeProperties keys the container evaluates itself later (e.g. the ``until`` condition is evaluated after every iteration)
    lazy_keys: ClassVar[tuple[str, ...]] = ()

    def evaluate_props(self, props: dict[str, Any], evaluate: Callable[[Any], Any]) -> dict[str, Any]:
        """Evaluate expression-valued properties while leaving nested activity definitions untouched."""
        return {k: (v if k in self.scope_keys or k in self.lazy_keys else evaluate(v)) for k, v in props.items()}

    def begin(self, actx: ActivityContext) -> dict[str, Any]:
        """Evaluate the container's own expressions once and return the state to persist (e.g. branch, items)."""
        raise NotImplementedError

    def scope_defs(self, props: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        """Activities every iteration scope runs, given the persisted state."""
        raise NotImplementedError

    def iterations(self, state: dict[str, Any]) -> int | None:
        """Number of iteration scopes (``None`` = open-ended, see :meth:`after_iteration`)."""
        return 1

    def parallelism(self, state: dict[str, Any]) -> int:
        return 1

    def iteration_context(self, state: dict[str, Any], iteration: int) -> dict[str, Any]:
        """Extra ``EvalContext.child(**kw)`` arguments for an iteration (e.g. the current item)."""
        return {}

    def after_iteration(self, actx: ActivityContext, state: dict[str, Any], iteration: int, ok: bool) -> bool:
        """Open-ended containers: called when an iteration finished; return True to start another one."""
        return False

    def finalize(self, state: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        """Build the container output from per-iteration results ``[{"index", "state", "outputs": {name: output}}]``."""
        return {"iterations": len(results)}


class ActivityRegistry:
    """Name -> :class:`Activity` instance. Thread-safe; built-ins plus entry-point plugins."""

    def __init__(self) -> None:
        self._types: dict[str, Activity] = {}
        self._lock = threading.RLock()
        self.rejected: list[dict[str, str]] = []
        self.plugins: list[str] = []

    def register(self, activity: Activity | type[Activity], *, source: str = "builtin") -> None:
        inst = activity() if isinstance(activity, type) else activity
        if not inst.type or not inst.type.replace("_", "").isalnum() or inst.type != inst.type.lower():
            raise ValueError(f"activity type name {inst.type!r} must be lower_snake_case")
        if inst.api_version.split(".")[0] != ACTIVITY_API_VERSION.split(".")[0]:
            self.rejected.append({"type": inst.type, "source": source, "reason": f"activity API {inst.api_version} is not compatible with {ACTIVITY_API_VERSION}"})
            log.warning("activity type %s (%s) rejected: incompatible API %s", inst.type, source, inst.api_version)
            return
        if inst.mode not in ("blocking", "async", "inline", "container"):
            raise ValueError(f"activity type {inst.type}: unknown mode {inst.mode!r}")
        with self._lock:
            if inst.type in self._types and source != "builtin" and self._types[inst.type].__class__.__module__.startswith("olcf.pipelines.activities"):
                self.rejected.append({"type": inst.type, "source": source, "reason": "would shadow a built-in activity type"})
                log.warning("plugin activity %s from %s would shadow a built-in type; ignored", inst.type, source)
                return
            self._types[inst.type] = inst

    def get(self, type_: str) -> Activity | None:
        with self._lock:
            return self._types.get(type_)

    def require(self, type_: str) -> Activity:
        a = self.get(type_)
        if a is None:
            raise OlcfError(f"unknown activity type '{type_}'")
        return a

    def types(self) -> list[str]:
        with self._lock:
            return sorted(self._types)

    def all(self) -> list[Activity]:
        with self._lock:
            return [self._types[k] for k in sorted(self._types)]

    def load_entry_points(self) -> None:
        from importlib.metadata import entry_points

        try:
            eps = entry_points(group=ENTRY_POINT_GROUP)
        except Exception:  # noqa: BLE001
            log.exception("could not enumerate activity entry points")
            return
        for ep in eps:
            try:
                obj = ep.load()
                items: Iterable[Any] = [obj] if isinstance(obj, type) or isinstance(obj, Activity) else (obj() if callable(obj) else obj)
                for it in items:
                    self.register(it, source=f"entry-point:{ep.name}")
                self.plugins.append(ep.name)
            except Exception as exc:  # noqa: BLE001
                self.rejected.append({"type": ep.name, "source": f"entry-point:{ep.name}", "reason": f"{type(exc).__name__}: {exc}"})
                log.exception("activity plugin %s failed to load", ep.name)


# ---------------------------------------------------------------------------------------------------------------------------------------- schema helpers
EXPR_REF = {"$ref": "#/$defs/expression"}


def X(schema: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
    """``schema`` or an expression string (``@...``)."""
    return {"anyOf": [schema, EXPR_REF]}


def s_str(**kw: Any) -> dict[str, Any]:
    return X({"type": "string", **kw})


def s_int(minimum: int | None = None, maximum: int | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {"type": "integer"}
    if minimum is not None:
        d["minimum"] = minimum
    if maximum is not None:
        d["maximum"] = maximum
    return X(d)


def s_num(minimum: float | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {"type": "number"}
    if minimum is not None:
        d["minimum"] = minimum
    return X(d)


def s_bool() -> dict[str, Any]:
    return X({"type": "boolean"})


def s_obj(**kw: Any) -> dict[str, Any]:
    return X({"type": "object", **kw})


def s_arr(**kw: Any) -> dict[str, Any]:
    return X({"type": "array", **kw})


def obj(properties: dict[str, Any], required: list[str] | None = None, *, extra: bool = False) -> dict[str, Any]:
    d: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": extra}
    if required:
        d["required"] = required
    return d


def get_path(d: Any, dotted: str) -> Any:
    for part in dotted.split("."):
        if not isinstance(d, dict) or part not in d:
            return None
        d = d[part]
    return d
