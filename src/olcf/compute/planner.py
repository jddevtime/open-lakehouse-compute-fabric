# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure scheduling and resource-admission algorithms (no I/O, no clocks, no database).

Everything the scheduler *decides* lives here so it can be unit/property tested exhaustively and replayed deterministically:

* **Ordering**: priority (with aging so nothing starves), then weighted fair-share across workspaces, then FIFO.
* **Quotas**: queue paused / max concurrency / max cores / max memory, per-workspace limits (queue-level and workspace-level).
* **Constraints**: pool (and reserved pools), labels (all must match), explicit node, runtime ``ready`` on the node, provider,
  cordoned/draining/offline nodes are never used.
* **Admission**: each node exposes ``usable = total - reservation`` (OS/broker headroom, ``compute.reserve_cores`` and
  ``compute.reserve_memory_fraction``); a placement is only produced if driver cores/memory fit into what is *still free*
  on the node (running allocations are subtracted, live memory pressure is respected) and, for Spark Standalone, if the
  cluster has enough free executor cores/memory. Unsafe oversubscription is therefore impossible by construction.
* **Starvation protection**: a job that waited longer than ``starvation_seconds`` and is blocked on resources closes its queue to
  backfilling for jobs ranked behind it, so big jobs cannot be starved forever by a stream of small ones.

* **Single-node out of the box**: with the default provider (``auto``) a job runs on Spark Standalone whenever some eligible node can host a master + worker +
  driver within its reservations (:func:`standalone_unfit_reason`), and *automatically falls back to local mode* otherwise - the reason is returned in
  :attr:`Placement.note` and shows up in the job timeline. Requests that were not set explicitly (:attr:`JobView.flexible`) are scaled to the machine instead of
  being rejected (:func:`local_default_sizing`, :func:`flex_executors`): a job is never refused just because the machine is small.

``plan`` is deterministic: the same inputs (in any order) yield the same output.
"""

from __future__ import annotations

import heapq
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

SCHEDULABLE_STATES = frozenset({"online", "degraded"})

#: A node can host Spark Standalone (master + worker daemons next to the driver and one executor) only with at least this much *usable* capacity.
STANDALONE_MIN_USABLE_CORES = 2
STANDALONE_MIN_USABLE_MEMORY_MB = 2048
#: Default (not explicitly requested) resources are scaled to the machine within these bounds.
LOCAL_DEFAULT_MAX_CORES = 4
LOCAL_DEFAULT_MAX_MEMORY_MB = 4096
FLEX_MAX_CLUSTER_CORES = 8
FLEX_MAX_EXECUTOR_CORES = 4
MIN_VIABLE_DRIVER_MB = 512

# --------------------------------------------------------------------------- views


@dataclass(frozen=True)
class NodeView:
    """What the planner needs to know about one worker node."""

    id: str
    name: str
    pool: str = "default"
    labels: Mapping[str, str] = field(default_factory=dict)
    state: str = "online"
    scheduling: str = "active"
    cores_total: int = 0
    memory_mb_total: int = 0
    reserve_cores: int = 1
    reserve_memory_fraction: float = 0.15
    #: cores/memory already promised on the node: running drivers, kernels and Spark daemon reservations
    cores_allocated: int = 0
    memory_allocated_mb: int = 0
    #: physical memory available according to the last (fresh) heartbeat, ``None`` = unknown
    live_available_memory_mb: int | None = None
    live_headroom_mb: int = 256
    runtimes_ready: frozenset[str] = frozenset()
    ports_used: frozenset[int] = frozenset()
    standalone_capable: bool = True

    @property
    def schedulable(self) -> bool:
        return self.state in SCHEDULABLE_STATES and self.scheduling == "active"

    @property
    def usable_cores(self) -> int:
        if self.cores_total <= 0:
            return 0
        return max(1, self.cores_total - max(0, self.reserve_cores))

    @property
    def usable_memory_mb(self) -> int:
        return int(self.memory_mb_total * (1.0 - min(0.9, max(0.0, self.reserve_memory_fraction))))

    @property
    def free_cores(self) -> int:
        return max(0, self.usable_cores - self.cores_allocated)

    @property
    def free_memory_mb(self) -> int:
        planned = self.usable_memory_mb - self.memory_allocated_mb
        if self.live_available_memory_mb is not None:
            planned = min(planned, self.live_available_memory_mb - self.live_headroom_mb)
        return max(0, planned)


@dataclass(frozen=True)
class ClusterView:
    """Executor capacity of one Spark Standalone cluster (runtime x pool). ``virtual`` = would be provisioned on demand."""

    runtime_id: str
    pool: str
    provider: str = "standalone"
    cluster_id: str | None = None
    worker_cores: Mapping[str, int] = field(default_factory=dict)  # node id -> advertised Spark worker cores
    worker_memory_mb: Mapping[str, int] = field(default_factory=dict)
    cores_used: int = 0
    memory_used_mb: int = 0
    virtual: bool = False

    @property
    def cores_total(self) -> int:
        return sum(self.worker_cores.values())

    @property
    def free_cores(self) -> int:
        return max(0, self.cores_total - self.cores_used)

    @property
    def memory_total_mb(self) -> int:
        return sum(self.worker_memory_mb.values())

    @property
    def free_memory_mb(self) -> int:
        return max(0, self.memory_total_mb - self.memory_used_mb)

    @property
    def max_worker_memory_mb(self) -> int:
        return max(self.worker_memory_mb.values(), default=0)


@dataclass(frozen=True)
class JobView:
    """What the planner needs to know about one queued job: its queue, priority, runtime, engine and resource request, placement constraints and age.
    """
    id: str
    workspace_id: str
    queue: str = "default"
    priority: int = 50
    queued_at: float = 0.0  # epoch seconds
    runtime_id: str | None = None
    engine: str = "spark"  # spark | python
    driver_cores: int = 1
    driver_memory_mb: int = 1024
    executor_cores: int = 1
    executor_memory_mb: int = 1024
    min_executors: int = 1
    max_executors: int | None = None
    max_cores: int | None = None
    dynamic_allocation: bool = False
    pool: str | None = None
    labels: Mapping[str, str] = field(default_factory=dict)
    node: str | None = None  # worker id or name
    provider: str | None = None
    not_before: float | None = None
    #: True when the submitter did not size the job (defaults were applied): the planner may grow or shrink the request to fit the machine.
    flexible: bool = False

    # ---- derived executor demand (Spark engine only)
    @property
    def min_executor_cores(self) -> int:
        return max(1, self.min_executors) * self.executor_cores

    @property
    def wanted_executor_cores(self) -> int:
        if self.max_cores:
            return max(self.max_cores, self.executor_cores)
        if self.max_executors:
            return max(self.max_executors, 1) * self.executor_cores
        return max(self.min_executor_cores, self.executor_cores)


@dataclass(frozen=True)
class RunningView:
    """An allocation that is currently consuming quota (a provisioning/running job or an active kernel)."""

    workspace_id: str
    queue: str = "default"
    cores: int = 0
    memory_mb: int = 0


@dataclass(frozen=True)
class QueueView:
    """A queue as the planner sees it: its limits, per-workspace limits, fair-share weights, allowed pools and whether it is paused."""
    name: str = "default"
    max_concurrency: int | None = None
    max_cores: int | None = None
    max_memory_mb: int | None = None
    #: {workspace_id | "*": {"max_concurrency": n, "max_cores": n}}
    workspace_limits: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    fair_share_weights: Mapping[str, float] = field(default_factory=dict)
    allowed_pools: tuple[str, ...] = ()
    paused: bool = False


@dataclass(frozen=True)
class PoolView:
    """A worker pool as the planner sees it; a reserved pool admits only its listed workspaces."""
    name: str
    reserved: bool = False
    workspace_ids: frozenset[str] = frozenset()

    def admits(self, workspace_id: str) -> bool:
        return not self.reserved or not self.workspace_ids or workspace_id in self.workspace_ids


@dataclass(frozen=True)
class PlannerConfig:
    """Tunables of the planner: aging and starvation limits, default provider, spread or pack strategy, driver headroom and the Spark port ranges.
    """
    starvation_seconds: float = 300.0
    aging_interval_seconds: float = 60.0
    aging_max_bonus: int = 30
    default_provider: str = "auto"  # auto | standalone | local | <name>
    strategy: str = "spread"  # spread (most free cores first) | pack (least free that fits)
    driver_reserve_fraction: float = 0.25
    driver_port_range: tuple[int, int] = (41000, 41099)
    block_manager_port_range: tuple[int, int] = (41100, 41199)
    max_candidates: int = 5000
    #: Providers executed by the controller on its own (no worker node needed), e.g. kubernetes and fabric.
    remote_providers: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Placement:
    """The planner's decision for one job: provider, driver node, pool, runtime, and the cores, memory and ports granted to the driver and executors.
    """
    job_id: str
    provider: str
    node_id: str | None  # driver node (None for remote providers)
    pool: str | None
    runtime_id: str | None
    driver_cores: int
    driver_memory_mb: int
    executor_cores_total: int = 0  # granted Spark cores (``spark.cores.max``)
    executor_cores: int = 1
    executor_memory_mb: int = 1024
    executors: int = 0
    driver_port: int | None = None
    block_manager_port: int | None = None
    cluster_key: tuple[str, str] | None = None  # (runtime_id, pool) for standalone
    #: Human readable remark about the decision (e.g. why the default provider fell back to local mode, how defaults were scaled); shown in the job timeline.
    note: str | None = None

    def granted(self) -> dict[str, Any]:
        return {
            "driver_cores": self.driver_cores, "driver_memory_mb": self.driver_memory_mb,
            "executor_cores_total": self.executor_cores_total, "executor_cores": self.executor_cores,
            "executor_memory_mb": self.executor_memory_mb, "executors": self.executors,
            "driver_port": self.driver_port, "block_manager_port": self.block_manager_port,
            "pool": self.pool, "node_id": self.node_id, "note": self.note,
        }


@dataclass(frozen=True)
class Blocked:
    """Why a queued job could not be placed in this pass, as a stable code (paused, quota, no_runtime, no_node, constraints, resources, held, backoff, infeasible) and a sentence.
    """
    job_id: str
    code: str  # paused | quota | no_runtime | no_node | constraints | resources | held | backoff | infeasible
    reason: str


@dataclass
class PlanResult:
    """The outcome of one planning pass: the placements to start and the jobs that stay queued, with reasons."""
    placements: list[Placement] = field(default_factory=list)
    blocked: list[Blocked] = field(default_factory=list)


# --------------------------------------------------------------------------- helpers


def allocate_port(rng: tuple[int, int], used: set[int] | frozenset[int]) -> int | None:
    """Lowest free port in the inclusive range, or ``None`` when exhausted."""
    for p in range(rng[0], rng[1] + 1):
        if p not in used:
            return p
    return None


def standalone_worker_sizing(node: NodeView, runtimes_sharing: int, driver_reserve_fraction: float = 0.25) -> tuple[int, int]:
    """(cores, memory_mb) a Spark worker daemon on ``node`` should advertise.

    The node's *usable* resources are split into a driver share (so drivers and kernels always have room next to the daemons)
    and an executor share, which is divided between the runtimes that run clusters on the node. ``(0, 0)`` means the node is
    too small for Spark Standalone (needs >= 2 usable cores) and must be left out.
    """
    uc, um = node.usable_cores, node.usable_memory_mb
    if uc < 2 or um < 1024:
        return 0, 0
    drv_cores = max(1, int(uc * driver_reserve_fraction))
    exec_cores = uc - drv_cores
    drv_mem = min(max(1024, int(um * driver_reserve_fraction)), um // 2)
    exec_mem = um - drv_mem
    share = max(1, runtimes_sharing)
    cores, mem = exec_cores // share, exec_mem // share
    if cores < 1 or mem < 512:
        return 0, 0
    return cores, mem


def standalone_unfit_reason(node: NodeView, runtimes_sharing: int = 1, driver_reserve_fraction: float = 0.25) -> str | None:
    """Why ``node`` cannot host Spark Standalone (master + worker + driver within its reservations), or ``None`` when it can."""
    if not node.standalone_capable:
        return f"node '{node.name}' cannot host Spark daemons"
    uc, um = node.usable_cores, node.usable_memory_mb
    if uc < STANDALONE_MIN_USABLE_CORES or um < STANDALONE_MIN_USABLE_MEMORY_MB:
        return (f"node '{node.name}' has {uc} usable core(s) and {um} MB usable memory after reservations, Spark Standalone (master + worker + driver) "
                f"needs at least {STANDALONE_MIN_USABLE_CORES} cores and {STANDALONE_MIN_USABLE_MEMORY_MB} MB")
    cores, mem = standalone_worker_sizing(node, runtimes_sharing, driver_reserve_fraction)
    if cores < 1 or mem < 512:
        return f"node '{node.name}' is shared by {runtimes_sharing} runtimes and cannot host a worker for each"
    return None


def local_default_sizing(node: NodeView) -> tuple[int, int]:
    """(driver cores, driver memory MB) a job without an explicit request gets in local mode: about half of the usable cores (<= 4) and a quarter of the memory (<= 4 GiB)."""
    uc, um = node.usable_cores, node.usable_memory_mb
    cores = max(1, min(LOCAL_DEFAULT_MAX_CORES, (uc + 1) // 2, uc))
    mem = min(max(1024, um // 4), LOCAL_DEFAULT_MAX_MEMORY_MB, max(256, um // 2))
    return cores, mem


def flex_driver(job: JobView, node: NodeView, provider: str) -> tuple[int, int]:
    """Driver cores/memory for a flexible job on ``node``: the ideal size for the provider, shrunk to what is currently free (never below 1 core / 512 MB)."""
    if provider == "local" and job.engine == "spark":
        ideal_c, ideal_m = local_default_sizing(node)
    else:  # python jobs and standalone drivers: keep the default request, only shrink it
        ideal_c, ideal_m = job.driver_cores, min(job.driver_memory_mb, max(MIN_VIABLE_DRIVER_MB, node.usable_memory_mb // 2))
    return max(1, min(ideal_c, node.free_cores)), max(MIN_VIABLE_DRIVER_MB, min(ideal_m, node.free_memory_mb))


def flex_executors(job: JobView, cl: ClusterView) -> JobView | None:
    """Scale the executor request of a flexible job to ``cl``: about half of the cluster cores (<= 8) in executors of <= 4 cores, shrunk to what is free.

    ``None`` when not even one executor of the original minimum size fits.
    """
    free_c, free_m = cl.free_cores, cl.free_memory_mb
    max_worker_c = max(cl.worker_cores.values(), default=0)
    max_worker_m = cl.max_worker_memory_mb
    if free_c < job.min_executor_cores or max_worker_c < 1 or max_worker_m < 512:
        return None
    ideal = max(job.min_executor_cores, min(FLEX_MAX_CLUSTER_CORES, max(1, cl.cores_total // 2)))
    target = min(ideal, free_c)
    ex_cores = max(1, min(FLEX_MAX_EXECUTOR_CORES, target, max_worker_c))
    execs = max(1, target // ex_cores)
    ex_mem = min(max_worker_m, max(job.executor_memory_mb, 1024 * ex_cores))
    while execs > 1 and execs * ex_mem > free_m:
        execs -= 1
    if execs * ex_mem > free_m:
        ex_mem = free_m // execs
    if ex_mem < 512:
        return None
    return replace(job, executor_cores=ex_cores, executor_memory_mb=ex_mem, min_executors=1, max_executors=execs, max_cores=execs * ex_cores)


def effective_priority(job: JobView, now: float, cfg: PlannerConfig) -> int:
    """Priority plus an aging bonus (1 point per ``aging_interval_seconds`` waited, capped)."""
    waited = max(0.0, now - job.queued_at)
    bonus = min(cfg.aging_max_bonus, int(waited // cfg.aging_interval_seconds)) if cfg.aging_interval_seconds > 0 else 0
    return job.priority + bonus


def _labels_match(want: Mapping[str, Any], have: Mapping[str, str]) -> bool:
    for k, v in want.items():
        if k not in have:
            return False
        if v not in (None, "*") and str(have[k]) != str(v):
            return False
    return True


def infeasible_reason(job: JobView, nodes: Sequence[NodeView], cfg: PlannerConfig | None = None) -> str | None:
    """Why ``job`` could *never* run on the registered fleet (ignoring current load / liveness), or ``None``.

    Used to reject unsafe/impossible requests up front instead of queueing them forever. Returns ``None`` when no node is
    registered yet (the fleet may still be coming up).
    """
    cfg = cfg or PlannerConfig()
    pool_nodes = [n for n in nodes if job.pool in (None, n.pool) and (job.node in (None, n.id, n.name)) and _labels_match(job.labels, n.labels)]
    if not nodes:
        return None
    if not pool_nodes:
        return "no registered node matches the placement constraints (pool/labels/node)"
    best_c = max(n.usable_cores for n in pool_nodes)
    best_m = max(n.usable_memory_mb for n in pool_nodes)
    if job.flexible:  # defaults are scaled to the machine: only a node without any usable capacity is hopeless
        if best_c < 1 or best_m < MIN_VIABLE_DRIVER_MB:
            return f"the largest eligible node has {best_c} usable cores and {best_m} MB usable memory, not enough to run any Spark driver"
        return None
    if job.driver_cores > best_c:
        return f"driver needs {job.driver_cores} cores but the largest eligible node has {best_c} usable cores (after reserving headroom)"
    if job.driver_memory_mb > best_m:
        return f"driver needs {job.driver_memory_mb} MB but the largest eligible node has {best_m} MB usable (after reserving headroom)"
    # ``auto`` can always fall back to local mode, so only an explicit standalone request can be infeasible because of the executors
    if job.engine == "spark" and (job.provider or cfg.default_provider) == "standalone":
        sizes = [standalone_worker_sizing(n, 1, cfg.driver_reserve_fraction) for n in pool_nodes]
        if not any(c >= job.executor_cores and m >= job.executor_memory_mb for c, m in sizes):
            return (f"executors need {job.executor_cores} cores / {job.executor_memory_mb} MB each but no eligible node can host a "
                    f"Spark worker that large")
    return None


# --------------------------------------------------------------------------- the planner


class _State:
    """Mutable bookkeeping while one plan is computed."""

    def __init__(self, nodes: Sequence[NodeView], clusters: Sequence[ClusterView], running: Sequence[RunningView]) -> None:
        self.nodes: dict[str, NodeView] = {n.id: n for n in nodes}
        self.clusters: dict[tuple[str, str], ClusterView] = {(c.runtime_id, c.pool): c for c in clusters if c.provider == "standalone"}
        self.q_running: dict[str, int] = defaultdict(int)
        self.q_cores: dict[str, int] = defaultdict(int)
        self.q_mem: dict[str, int] = defaultdict(int)
        self.qw_running: dict[tuple[str, str], int] = defaultdict(int)
        self.qw_cores: dict[tuple[str, str], int] = defaultdict(int)
        self.ws_cores: dict[str, float] = defaultdict(float)
        for r in running:
            self.q_running[r.queue] += 1
            self.q_cores[r.queue] += r.cores
            self.q_mem[r.queue] += r.memory_mb
            self.qw_running[(r.queue, r.workspace_id)] += 1
            self.qw_cores[(r.queue, r.workspace_id)] += r.cores
            self.ws_cores[r.workspace_id] += r.cores

    def commit(self, job: JobView, p: Placement) -> None:
        cores = p.driver_cores + p.executor_cores_total
        mem = p.driver_memory_mb + p.executor_memory_mb * max(p.executors, 0)
        self.q_running[job.queue] += 1
        self.q_cores[job.queue] += cores
        self.q_mem[job.queue] += mem
        self.qw_running[(job.queue, job.workspace_id)] += 1
        self.qw_cores[(job.queue, job.workspace_id)] += cores
        self.ws_cores[job.workspace_id] += cores
        if p.node_id:
            n = self.nodes[p.node_id]
            ports = set(n.ports_used)
            ports.update(x for x in (p.driver_port, p.block_manager_port) if x)
            self.nodes[p.node_id] = replace(n, cores_allocated=n.cores_allocated + p.driver_cores,
                                            memory_allocated_mb=n.memory_allocated_mb + p.driver_memory_mb, ports_used=frozenset(ports))
        if p.cluster_key and p.cluster_key in self.clusters:
            c = self.clusters[p.cluster_key]
            self.clusters[p.cluster_key] = replace(c, cores_used=c.cores_used + p.executor_cores_total,
                                                   memory_used_mb=c.memory_used_mb + p.executor_memory_mb * p.executors)


def _quota_block(job: JobView, q: QueueView, st: _State, ws_quota: Mapping[str, Mapping[str, int]]) -> Blocked | None:
    if q.paused:
        return Blocked(job.id, "paused", f"queue '{q.name}' is paused")
    if q.max_concurrency is not None and st.q_running[q.name] >= q.max_concurrency:
        return Blocked(job.id, "quota", f"queue '{q.name}' is at its concurrency limit ({q.max_concurrency})")
    if q.max_cores is not None and st.q_cores[q.name] + job.driver_cores > q.max_cores:
        return Blocked(job.id, "quota", f"queue '{q.name}' would exceed its core limit ({q.max_cores})")
    if q.max_memory_mb is not None and st.q_mem[q.name] + job.driver_memory_mb > q.max_memory_mb:
        return Blocked(job.id, "quota", f"queue '{q.name}' would exceed its memory limit ({q.max_memory_mb} MB)")
    for source, limits in (("queue", q.workspace_limits.get(job.workspace_id) or q.workspace_limits.get("*") or {}),
                           ("workspace", ws_quota.get(job.workspace_id) or {})):
        mc = limits.get("max_concurrency")
        if mc is not None and st.qw_running[(q.name, job.workspace_id)] >= mc:
            return Blocked(job.id, "quota", f"workspace is at its {source} concurrency limit ({mc})")
        mcores = limits.get("max_cores")
        if mcores is not None and st.qw_cores[(q.name, job.workspace_id)] + job.driver_cores > mcores:
            return Blocked(job.id, "quota", f"workspace would exceed its {source} core limit ({mcores})")
    return None


def _node_pool_ok(job: JobView, n: NodeView, q: QueueView, pools: Mapping[str, PoolView]) -> bool:
    if job.pool is not None and n.pool != job.pool:
        return False
    if q.allowed_pools and n.pool not in q.allowed_pools:
        return False
    pv = pools.get(n.pool)
    return pv is None or pv.admits(job.workspace_id)


def _resolve_provider(job: JobView, eligible: Sequence[NodeView], cfg: PlannerConfig, sharing: Mapping[str, int] | None = None) -> tuple[str, str | None]:
    """(provider, note). ``auto``: Spark Standalone when an eligible node can host master + worker + driver, else local mode with the reason as ``note``."""
    wanted = job.provider or cfg.default_provider
    if job.engine == "python":
        return (wanted if wanted in cfg.remote_providers else "local"), None
    if wanted == "auto":
        reasons: list[str] = []
        for n in sorted(eligible, key=lambda x: x.id):
            why = standalone_unfit_reason(n, (sharing or {}).get(n.id, 1), cfg.driver_reserve_fraction)
            if why is None:
                return "standalone", None
            reasons.append(why)
        detail = "; ".join(dict.fromkeys(reasons))[:400]
        return "local", f"running Spark in local mode instead of a standalone cluster: {detail}"
    return wanted, None


def _score(strategy: str, n: NodeView) -> tuple:
    if strategy == "pack":
        return (n.free_cores, n.free_memory_mb, n.name, n.id)
    return (-n.free_cores, -n.free_memory_mb, n.name, n.id)


def _cap_to_core_quota(job: JobView, p: Placement, q: QueueView, st: _State, ws_quota: Mapping[str, Mapping[str, int]]) -> Placement | Blocked:
    """Shrink the executor grant so queue/workspace *core* limits (which count driver + executors) are never exceeded."""
    limits: list[int] = []
    if q.max_cores is not None:
        limits.append(q.max_cores - st.q_cores[q.name])
    for lim, used in ((q.workspace_limits.get(job.workspace_id) or q.workspace_limits.get("*") or {}, st.qw_cores[(q.name, job.workspace_id)]),
                      (ws_quota.get(job.workspace_id) or {}, st.qw_cores[(q.name, job.workspace_id)])):
        if lim.get("max_cores") is not None:
            limits.append(lim["max_cores"] - used)
    if not limits or p.executor_cores_total == 0:
        return p
    room = min(limits) - p.driver_cores
    if room >= p.executor_cores_total:
        return p
    per = max(1, p.executor_cores)
    grant = room - room % per
    if p.provider == "standalone" and grant >= job.min_executor_cores:
        return replace(p, executor_cores_total=grant, executors=grant // per)
    return Blocked(job.id, "quota", f"queue/workspace core limit leaves {max(room, 0)} cores for executors, job needs at least {job.min_executor_cores}")


def _try_place(job: JobView, q: QueueView, st: _State, pools: Mapping[str, PoolView], cfg: PlannerConfig,
               runtime_sharing: Mapping[str, int]) -> Placement | Blocked:
    # ---- remote providers (kubernetes, fabric...) run without a worker node
    if job.provider and job.provider in cfg.remote_providers:
        return Placement(job.id, job.provider, None, job.pool, job.runtime_id, job.driver_cores, job.driver_memory_mb,
                         executor_cores_total=job.wanted_executor_cores, executor_cores=job.executor_cores,
                         executor_memory_mb=job.executor_memory_mb,
                         executors=max(1, job.wanted_executor_cores // max(1, job.executor_cores)))
    candidates = [n for n in st.nodes.values() if n.schedulable]
    if not candidates:
        return Blocked(job.id, "no_node", "no schedulable worker node is online (all nodes offline, cordoned or draining)")
    by_constraint = [n for n in candidates if _node_pool_ok(job, n, q, pools) and (job.node in (None, n.id, n.name)) and _labels_match(job.labels, n.labels)]
    if not by_constraint:
        return Blocked(job.id, "constraints", "no online node matches the placement constraints (pool/labels/node/reserved pool)")
    rt = job.runtime_id
    with_runtime = [n for n in by_constraint if rt is None or rt in n.runtimes_ready]
    if not with_runtime:
        return Blocked(job.id, "no_runtime", f"runtime '{rt}' is not ready on any eligible node (install or verify it on a worker)")
    provider, note = _resolve_provider(job, with_runtime, cfg, runtime_sharing)
    flex = job.flexible and job.engine == "spark"
    if flex:
        # defaults are sized per node: a job without an explicit request runs with whatever the machine can give it (never below 1 core / 512 MB)
        sized = {n.id: flex_driver(job, n, provider) for n in with_runtime}
        fits = [n for n in with_runtime if n.free_cores >= 1 and n.free_memory_mb >= MIN_VIABLE_DRIVER_MB]
    else:
        sized = {}
        fits = [n for n in with_runtime if n.free_cores >= job.driver_cores and n.free_memory_mb >= job.driver_memory_mb]
    if not fits:
        best = max(with_runtime, key=lambda n: (n.free_cores, n.free_memory_mb))
        need_c, need_m = (1, MIN_VIABLE_DRIVER_MB) if flex else (job.driver_cores, job.driver_memory_mb)
        return Blocked(job.id, "resources", (f"waiting for resources: driver needs {need_c} cores / {need_m} MB, "
                                             f"best eligible node '{best.name}' has {best.free_cores} cores / {best.free_memory_mb} MB free"))

    def driver_of(n: NodeView) -> tuple[int, int]:
        return sized[n.id] if flex else (job.driver_cores, job.driver_memory_mb)

    def sized_note(n: NodeView) -> str | None:
        if not flex:
            return note
        c, m = driver_of(n)
        scaled = f"default resources scaled to the machine: driver {c} core(s) / {m} MB"
        return f"{note}; {scaled}" if note else scaled

    # ---- driver placement per provider
    if job.engine == "python" or provider != "standalone":
        n = min(fits, key=lambda x: _score(cfg.strategy, x))
        dport = bport = None
        if job.engine == "spark":
            # local[*] and external masters still open block-manager/driver RPC ports on the node
            dport = allocate_port(cfg.driver_port_range, n.ports_used)
            bport = allocate_port(cfg.block_manager_port_range, n.ports_used)
        dc, dm = driver_of(n)
        return Placement(job.id, provider, n.id, n.pool, rt, dc, dm, driver_port=dport, block_manager_port=bport, note=sized_note(n))
    # ---- Spark Standalone: pick (driver node, cluster) so that executors also fit
    best_c: tuple[tuple, NodeView, ClusterView, int, JobView] | None = None
    reasons: list[str] = []
    for n in sorted(fits, key=lambda x: _score(cfg.strategy, x)):
        key = (rt or "", n.pool)
        cl = st.clusters.get(key)
        if cl is None:
            # virtual cluster: what would be provisioned on every capable eligible node of the pool
            wc, wm = {}, {}
            for m in candidates:
                if m.pool == n.pool and (rt is None or rt in m.runtimes_ready) and m.standalone_capable:
                    c_, m_ = standalone_worker_sizing(m, runtime_sharing.get(m.id, 1), cfg.driver_reserve_fraction)
                    if c_ > 0:
                        wc[m.id], wm[m.id] = c_, m_
            cl = ClusterView(rt or "", n.pool, "standalone", None, wc, wm, 0, 0, virtual=True)
            st.clusters[key] = cl  # later jobs of this plan must see the capacity this one consumes
        if cl.cores_total == 0:
            reasons.append(f"pool '{n.pool}' has no node large enough for Spark Standalone workers (needs >= 2 usable cores)")
            continue
        jx = job
        if flex:
            scaled = flex_executors(job, cl)
            if scaled is None:
                reasons.append(f"cluster for runtime '{rt}' in pool '{n.pool}' has {cl.free_cores} free executor cores / {cl.free_memory_mb} MB free")
                continue
            jx = scaled
        need_min = jx.min_executor_cores
        grant = min(jx.wanted_executor_cores, cl.free_cores)
        grant -= grant % jx.executor_cores  # whole executors only
        if jx.executor_memory_mb > cl.max_worker_memory_mb:
            reasons.append(f"executor memory {jx.executor_memory_mb} MB exceeds the largest Spark worker ({cl.max_worker_memory_mb} MB) in pool '{n.pool}'")
            continue
        if grant < need_min:
            reasons.append(f"cluster for runtime '{rt}' in pool '{n.pool}' has {cl.free_cores} free executor cores, job needs at least {need_min}")
            continue
        execs = grant // jx.executor_cores
        if execs * jx.executor_memory_mb > cl.free_memory_mb:
            execs = max(jx.min_executors, cl.free_memory_mb // max(1, jx.executor_memory_mb))
            grant = execs * jx.executor_cores
            if grant < need_min or execs * jx.executor_memory_mb > cl.free_memory_mb:
                reasons.append(f"cluster in pool '{n.pool}' has only {cl.free_memory_mb} MB free executor memory")
                continue
        score = _score(cfg.strategy, n)
        cand = (score, n, cl, grant, jx)
        if best_c is None or score < best_c[0]:
            best_c = cand
    if best_c is None:
        return Blocked(job.id, "resources", "waiting for resources: " + ("; ".join(dict.fromkeys(reasons)) if reasons else "no capacity in any Spark cluster"))
    _, n, cl, grant, jx = best_c
    dport = allocate_port(cfg.driver_port_range, n.ports_used)
    bport = allocate_port(cfg.block_manager_port_range, n.ports_used | ({dport} if dport else set()))
    if dport is None or bport is None:
        return Blocked(job.id, "resources", f"node '{n.name}' has no free driver/block-manager port left in the configured range")
    execs = grant // jx.executor_cores
    dc, dm = driver_of(n)
    extra = sized_note(n)
    if flex:
        extra = f"{extra}, {execs} executor(s) x {jx.executor_cores} core(s) / {jx.executor_memory_mb} MB"
    return Placement(job.id, "standalone", n.id, n.pool, rt, dc, dm, executor_cores_total=grant,
                     executor_cores=jx.executor_cores, executor_memory_mb=jx.executor_memory_mb, executors=execs,
                     driver_port=dport, block_manager_port=bport, cluster_key=(rt or "", n.pool), note=extra)


def plan(jobs: Sequence[JobView], nodes: Sequence[NodeView], clusters: Sequence[ClusterView] = (), queues: Mapping[str, QueueView] | None = None,
         running: Sequence[RunningView] = (), pools: Mapping[str, PoolView] | None = None, *, now: float, cfg: PlannerConfig | None = None,
         workspace_quotas: Mapping[str, Mapping[str, int]] | None = None, runtimes_per_node: Mapping[str, int] | None = None) -> PlanResult:
    """Decide which queued jobs start now. Pure and deterministic.

    ``runtimes_per_node`` maps node id -> number of runtimes that run Spark clusters on it (used to size virtual clusters).
    """
    cfg = cfg or PlannerConfig()
    queues = dict(queues or {})
    pools = dict(pools or {})
    ws_quota = workspace_quotas or {}
    out = PlanResult()
    st = _State(sorted(nodes, key=lambda n: n.id), sorted(clusters, key=lambda c: (c.runtime_id, c.pool)), running)
    sharing = {nid: max(1, k) for nid, k in (runtimes_per_node or {}).items()}

    ordered = sorted(jobs, key=lambda j: j.id)[: cfg.max_candidates * 4]
    cand: list[JobView] = []
    for j in ordered:
        if j.not_before is not None and j.not_before > now:
            out.blocked.append(Blocked(j.id, "backoff", "waiting for retry back-off to elapse"))
        else:
            cand.append(j)
    cand = cand[: cfg.max_candidates]

    # per-workspace queues sorted by (effective priority desc, queued_at, id); heap picks across workspaces
    per_ws: dict[str, deque[JobView]] = defaultdict(deque)
    for j in sorted(cand, key=lambda j: (-effective_priority(j, now, cfg), j.queued_at, j.id)):
        per_ws[j.workspace_id].append(j)

    def weight(j: JobView) -> float:
        q = queues.get(j.queue)
        w = (q.fair_share_weights.get(j.workspace_id) if q else None) or 1.0
        return max(0.01, float(w))

    def key(j: JobView) -> tuple:
        return (-effective_priority(j, now, cfg), st.ws_cores[j.workspace_id] / weight(j), j.queued_at, j.id)

    heap: list[tuple[tuple, str]] = []
    for ws, dq in sorted(per_ws.items()):
        heapq.heappush(heap, (key(dq[0]), ws))
    queue_barrier: dict[str, str] = {}  # queue -> id of the starving job holding the queue

    while heap:
        _, ws = heapq.heappop(heap)
        dq = per_ws[ws]
        job = dq.popleft()
        q = queues.get(job.queue) or QueueView(job.queue)
        res: Placement | Blocked
        if job.queue in queue_barrier:
            res = Blocked(job.id, "held", f"held behind starving job {queue_barrier[job.queue]} (backfill closed for this queue)")
        else:
            qb = _quota_block(job, q, st, ws_quota)
            res = qb if qb is not None else _try_place(job, q, st, pools, cfg, sharing)
            if isinstance(res, Placement):
                res = _cap_to_core_quota(job, res, q, st, ws_quota)
        if isinstance(res, Placement):
            st.commit(job, res)
            out.placements.append(res)
        else:
            out.blocked.append(res)
            starving = (now - job.queued_at) >= cfg.starvation_seconds
            if starving and res.code == "resources" and job.queue not in queue_barrier:
                queue_barrier[job.queue] = job.id
        if dq:
            heapq.heappush(heap, (key(dq[0]), ws))
    return out
