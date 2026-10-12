# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The compute provider interface (MASTER_REQUIREMENTS section 14) and shared building blocks.

A provider turns a *placed* job into a running Spark application and can later report on it::

    discover()            capabilities, workers, runtime availability and health
    submit(job)           create the Spark/session/application and return an immutable handle
    status(handle)        normalised state, resources, progress and provider-native details
    logs(handle, ...)     paged driver/executor logs
    cancel(handle)        idempotent graceful, then forced, termination
    scale(handle, policy) optional executor scaling / dynamic allocation controls
    ui(handle)            secure Spark UI proxy metadata (never an unauthenticated URL for browsers)
    reconcile()           enumerate active applications after a control-plane restart

Two execution models exist:

* ``execution == "agent"`` (standalone, local, external master): the OLCF runner executes on a *driver node* chosen by the
  scheduler; ``prepare`` builds the ``spark`` section of the job spec (master URL, driver host/ports, executor sizing) and
  ``submit`` ships ``run_job`` to that node's agent.
* ``execution == "remote"`` (kubernetes, Fabric/Livy): the controller itself talks to the external system; there is no worker node.

Providers are pluggable through the entry-point group ``olcf.compute_providers`` (see :mod:`olcf.compute.providers`).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from olcf.core.errors import Unsupported

if TYPE_CHECKING:  # pragma: no cover
    from olcf.compute.hub import WorkerHub
    from olcf.compute.logstore import LogStore
    from olcf.compute.registry import ComputeRegistry
    from olcf.core.context import AppContext

log = logging.getLogger("olcf.compute.providers")

#: Normalised application states reported by ``status``.
APP_STATES = ("pending", "running", "succeeded", "failed", "cancelled", "unknown")
TERMINAL_APP_STATES = frozenset({"succeeded", "failed", "cancelled"})


@dataclass
class ProviderEnv:
    """Services a provider may use (injected, so providers stay testable)."""

    ctx: AppContext
    registry: ComputeRegistry
    hub: WorkerHub
    logs: LogStore

    def secret(self, secret_id: str | None) -> Any:
        return self.ctx.secrets.resolve(secret_id) if secret_id else None


@dataclass
class AppHandle:
    """Immutable reference to an application created by a provider."""

    provider: str
    job_id: str
    attempt: int
    application_id: str | None = None
    worker_id: str | None = None
    cluster_id: str | None = None
    native: dict[str, Any] = field(default_factory=dict)  # provider-native ids (pod name, Livy batch id ...)

    def to_dict(self) -> dict[str, Any]:
        return {"provider": self.provider, "job_id": self.job_id, "attempt": self.attempt, "application_id": self.application_id,
                "worker_id": self.worker_id, "cluster_id": self.cluster_id, "native": self.native}

    @classmethod
    def from_job(cls, job: dict[str, Any]) -> AppHandle:
        return cls(provider=job.get("provider") or "", job_id=job["id"], attempt=job.get("attempt", 1), application_id=job.get("application_id"),
                   worker_id=job.get("driver_worker_id"), cluster_id=job.get("cluster_id"), native=dict(job.get("provider_handle") or {}))


@dataclass
class AppStatus:
    """The normalised status of an application on a provider: state, progress, resources and the provider's own details."""
    state: str  # one of APP_STATES
    application_id: str | None = None
    detail: str = ""
    progress: float | None = None
    resources: dict[str, Any] = field(default_factory=dict)
    native: dict[str, Any] = field(default_factory=dict)
    exit_code: int | None = None
    job_id: str | None = None  # filled by reconcile() when the application maps to an OLCF job
    #: Machine-readable reason for a state that needs an operator's eye (``submission_uncertain``, ``submission_not_created``, ``submission_unresolved`` ...);
    #: the controller records it as a job event, once per change.
    code: str | None = None
    #: Terminal results only. ``result``: the complete result document when the provider itself is the source of truth (``result_contract == "provider"``).
    #: ``termination``: the compact summary a runner left in the platform's termination record (Kubernetes), a secondary completion channel that does not depend on
    #: log volume. ``confirmed_gone`` / ``reached_running`` refine the retry decision of a failure the provider reports (``None`` = the controller's default).
    result: dict[str, Any] | None = None
    termination: dict[str, Any] | None = None
    confirmed_gone: bool | None = None
    reached_running: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"state": self.state, "application_id": self.application_id, "detail": self.detail, "progress": self.progress,
                "resources": self.resources, "native": self.native, "exit_code": self.exit_code, "job_id": self.job_id, "code": self.code}


@dataclass
class LogsPage:
    """One page of driver or executor logs with the offset to continue from and whether the end was reached.

    ``cursor`` is an optional *opaque* JSON-serialisable position that a provider with a better notion than a line number (Kubernetes: timestamp + anchor) hands
    back; the controller persists it with the job and passes it to the next ``logs`` call. ``gap`` is a human-readable note when output that was already produced
    could no longer be read (log rotation / truncation on the remote side): the controller writes it into the job log so nothing is lost silently.
    """
    data: str
    next_offset: int
    eof: bool
    source: str = "driver"
    cursor: Any = None
    gap: str | None = None


@dataclass
class UiInfo:
    """Where the Spark UI lives. ``upstream`` is only ever dereferenced by the authenticated proxy."""

    upstream: str | None
    available: bool
    detail: str = ""
    proxy_hint: str = ""


@dataclass
class Discovery:
    """The result of ``discover()``: whether a provider is healthy, what it can do and which workers, runtimes and clusters it knows."""
    provider: str
    type: str
    ok: bool
    detail: str = ""
    capabilities: list[str] = field(default_factory=list)
    execution: str = "agent"
    workers: list[dict[str, Any]] = field(default_factory=list)
    runtimes: list[dict[str, Any]] = field(default_factory=list)
    clusters: list[dict[str, Any]] = field(default_factory=list)
    live_verified: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class PrepareRequest:
    """Everything a provider needs to build the ``spark`` section of the job spec."""

    job: dict[str, Any]  # jobs row
    placement: dict[str, Any]  # planner Placement.granted() + provider/cluster fields
    runtime: dict[str, Any]  # runtimes row (manifest, spark_conf_defaults ...)
    spark_conf: dict[str, str]  # approved user/environment conf (already validated against the allow-list)
    app_name: str
    node: Any | None = None  # NodeInfo of the driver node (agent execution)


@dataclass
class SubmitRequest:
    """Everything a provider needs to start a job: the job record, the complete job specification (sensitive; never logged), the source and the target.
    """
    job: dict[str, Any]
    spec: dict[str, Any]  # complete job spec (contains sensitive material; never log it)
    source_bytes: bytes | None = None
    source_path: str | None = None  # relative path inside the job workdir
    worker_id: str | None = None
    cluster_id: str | None = None
    timeout: float = 60.0


class ComputeProvider(ABC):
    """Base class for providers. Subclasses set ``type`` and implement the interface."""

    type: ClassVar[str] = ""
    #: Status flag surfaced in the API/docs: whether this provider has been exercised against the real external system.
    live_verified: ClassVar[bool] = True
    capabilities: ClassVar[tuple[str, ...]] = ()
    #: ``agent`` (runs on an OLCF worker) or ``remote`` (the controller talks to an external system); informational, see ``execution``.
    execution_model: ClassVar[str] = "agent"
    #: False when the target system brings its own runtime (Fabric, plain Livy): no OLCF runtime is resolved or required.
    uses_runtimes: ClassVar[bool] = True
    #: False when the target system builds the Spark session itself (Livy): ``prepare`` is not called.
    prepares_spark: ClassVar[bool] = True
    #: How a remote application delivers its result (CONTRACTS, PROV addenda). ``"runner"``: the OLCF runner runs inside the application and must emit the documented
    #: ``result`` event exactly once (Kubernetes pods); a terminal application without that result is *incomplete* and is reconciled, never reported as a success.
    #: ``"provider"``: the target system is itself the source of truth (Livy statement/batch state and output), so ``AppStatus.result`` is complete.
    result_contract: ClassVar[str] = "provider"

    def __init__(self, name: str, config: dict[str, Any], env: ProviderEnv) -> None:
        self.name, self.config, self.env = name, dict(config or {}), env

    @property
    def execution(self) -> str:
        return "agent"

    def validate_config(self) -> None:  # noqa: B027 - optional hook
        """Raise ``BadRequest`` for an unusable configuration (called when a provider is registered)."""

    # ---- the interface -------------------------------------------------------------------
    @abstractmethod
    def discover(self) -> Discovery: ...

    def prepare(self, req: PrepareRequest) -> dict[str, Any]:
        """Return the ``spark`` block for the job spec (agent execution) and ensure any cluster exists."""
        raise Unsupported(f"provider '{self.name}' does not prepare agent-executed jobs")

    def submission_intent(self, req: SubmitRequest) -> dict[str, Any] | None:
        """For remote providers whose create request is not idempotent: the provider-native handle fields that identify what *would* be created (for example the Livy
        ``session_name``). The controller persists them before calling :meth:`submit`, so a crash in between is reconciled against the remote system instead of replayed."""
        return None

    @abstractmethod
    def submit(self, req: SubmitRequest) -> AppHandle: ...

    @abstractmethod
    def status(self, handle: AppHandle) -> AppStatus: ...

    @abstractmethod
    def logs(self, handle: AppHandle, offset: int = 0, limit: int = 65536, *, cursor: Any = None) -> LogsPage: ...

    def find_result(self, handle: AppHandle) -> dict[str, Any] | None:
        """Scan the retained output of a *terminal* application for the runner's ``result`` event (reconcile path of ``result_contract == "runner"``).

        Providers whose results do not travel through logs return ``None``. The scan is independent of the ingestion cursor, so it also works after the cursor was
        lost or the log rotated; it must be bounded in work."""
        return None

    @abstractmethod
    def cancel(self, handle: AppHandle, *, force: bool = False, grace_seconds: int = 15) -> None: ...

    def scale(self, handle: AppHandle, policy: dict[str, Any]) -> dict[str, Any]:
        raise Unsupported(f"provider '{self.name}' ({self.type}) cannot rescale a running application",
                          remediation="request dynamic allocation in the job's ResourceRequest instead")

    @abstractmethod
    def ui(self, handle: AppHandle) -> UiInfo: ...

    @abstractmethod
    def reconcile(self) -> list[AppStatus]: ...

    # ---- hooks -------------------------------------------------------------------------
    def on_hello(self, worker: dict[str, Any], hello: Any) -> None:  # noqa: B027
        """An agent (re)connected and reported its state."""

    def on_daemon_state(self, worker_id: str, msg: Any) -> None:  # noqa: B027
        """An agent reported a Spark daemon state change."""

    def maintain(self) -> None:  # noqa: B027
        """Periodic leader-side upkeep (refresh cluster status, restart dead daemons ...)."""

    def health(self, deep: bool = False) -> list[dict[str, Any]]:
        return []

    def close(self) -> None:  # noqa: B027
        """Release threads/sockets (called when the provider is replaced or the control plane stops)."""


def spark_memory(mb: int) -> str:
    """Spark memory string (``1024`` MB -> ``1024m``)."""
    return f"{int(mb)}m"


def base_spark_conf(req: PrepareRequest) -> dict[str, str]:
    """Conf shared by every provider: runtime defaults, then approved user conf, then OLCF-owned resource keys."""
    conf: dict[str, str] = {str(k): str(v) for k, v in ((req.runtime.get("manifest") or {}).get("spark_conf_defaults") or {}).items()}
    conf.update(req.spark_conf)
    g = req.placement
    conf["spark.driver.memory"] = spark_memory(g.get("driver_memory_mb", 1024))
    conf["spark.driver.cores"] = str(g.get("driver_cores", 1))
    conf["spark.app.name"] = req.app_name
    return conf


def dynamic_allocation_conf(job_resources: dict[str, Any], placement: dict[str, Any]) -> dict[str, str]:
    """Spark dynamic allocation flags (shuffle tracking: needs no external shuffle service, works on standalone and k8s)."""
    if not job_resources.get("dynamic_allocation"):
        return {}
    ex_cores = max(1, int(placement.get("executor_cores") or 1))
    max_ex = job_resources.get("max_executors") or max(1, int(placement.get("executor_cores_total") or ex_cores) // ex_cores)
    return {
        "spark.dynamicAllocation.enabled": "true", "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": str(int(job_resources.get("min_executors") or 0)),
        "spark.dynamicAllocation.initialExecutors": str(max(int(job_resources.get("min_executors") or 0), 1)),
        "spark.dynamicAllocation.maxExecutors": str(int(max_ex)),
    }
