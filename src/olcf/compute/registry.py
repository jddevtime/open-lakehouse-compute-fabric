# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Database access for workers, pools, queues and providers, plus the node health model.

The hub (socket handling), scheduler (decisions) and topology API (presentation) all go through :class:`ComputeRegistry` so
there is exactly one place that knows how a ``workers`` row maps to what the planner needs (:class:`NodeInfo`) and how
heartbeats turn into ``online / degraded / offline``.
"""

from __future__ import annotations

import logging
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from olcf.compute import schema as C
from olcf.compute.planner import NodeView
from olcf.core.config import Settings
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.states import SchedulingState, WorkerState
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, insert_row, new_etag

log = logging.getLogger("olcf.compute.registry")

DEFAULT_POOL = "default"
DEFAULT_QUEUE = "default"
ACTIVE_JOB_STATES = ("provisioning", "running", "cancel_requested")
ACTIVE_SESSION_STATES = ("starting", "idle", "busy")
HISTORY_SAMPLES = 120


@dataclass
class NodeInfo:
    """Everything the scheduler/topology needs to know about a worker, derived from its row."""

    row: dict[str, Any]
    id: str
    name: str
    hostname: str | None
    address: str | None
    pool: str
    labels: dict[str, str]
    state: str
    scheduling: str
    cores: int
    memory_mb: int
    disk_gb: float
    runtimes: dict[str, dict[str, Any]] = field(default_factory=dict)
    connected: bool = False
    embedded: bool = False
    #: what the agent is *willing* to hand out (``--max-cores/--max-memory-mb``); never more than the machine.  0 = not advertised.
    cap_cores: int = 0
    cap_memory_mb: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> NodeInfo:
        inv = row.get("inventory") or {}
        return cls(
            row=row, id=row["id"], name=row["name"], hostname=row.get("hostname"), address=row.get("address"),
            pool=row.get("pool") or DEFAULT_POOL, labels={str(k): str(v) for k, v in (row.get("labels") or {}).items()},
            state=row["state"], scheduling=row["scheduling"], cores=int(inv.get("cores") or 0), memory_mb=int(inv.get("memory_mb") or 0),
            disk_gb=float(inv.get("disk_gb") or 0), runtimes={r["id"]: r for r in (row.get("runtimes") or []) if isinstance(r, dict) and "id" in r},
            connected=bool(row.get("connected")), embedded=bool(row.get("embedded")),
            cap_cores=int((inv.get("capacity") or {}).get("cores") or 0), cap_memory_mb=int((inv.get("capacity") or {}).get("memory_mb") or 0),
        )

    @property
    def sched_cores(self) -> int:
        """Cores the planner may plan with: the inventory, capped by the agent's advertised capacity."""
        return min(self.cores, self.cap_cores) if self.cap_cores and self.cores else (self.cap_cores or self.cores)

    @property
    def sched_memory_mb(self) -> int:
        return min(self.memory_mb, self.cap_memory_mb) if self.cap_memory_mb and self.memory_mb else (self.cap_memory_mb or self.memory_mb)

    @property
    def ready_runtimes(self) -> frozenset[str]:
        return frozenset(rid for rid, r in self.runtimes.items() if r.get("state") == "ready")

    def advertise_host(self, settings: Settings) -> str:
        """Host name other nodes use to reach this node (driver host, Spark master/worker address)."""
        override = (self.row.get("inventory") or {}).get("advertise_host") or self.labels.get("olcf.io/advertise-host")
        if override:
            return str(override)
        if self.embedded and settings.compute.advertise_host:
            return settings.compute.advertise_host
        return self.address or self.hostname or "127.0.0.1"


def assess_health(row: dict[str, Any], now_epoch: float, *, heartbeat_s: float, offline_after_s: float) -> tuple[str, list[str]]:
    """Derive the liveness state from a worker row. Pure function (unit tested).

    * never connected -> ``pending``; revoked stays ``revoked``;
    * no heartbeat for ``offline_after_s`` -> ``offline``;
    * late heartbeat (> 2x interval), failing components, <5% disk or <5% memory available -> ``degraded``.
    """
    if row.get("revoked_at") or row.get("state") == WorkerState.REVOKED.value:
        return WorkerState.REVOKED.value, ["revoked"]
    seen = row.get("last_seen_at")
    if not seen:
        return WorkerState.PENDING.value, ["never connected"]
    age = now_epoch - from_iso(seen).timestamp()
    if age > offline_after_s:
        return WorkerState.OFFLINE.value, [f"no heartbeat for {age:.0f}s"]
    reasons: list[str] = []
    if age > max(2 * heartbeat_s, 3.0):
        reasons.append(f"late heartbeat ({age:.0f}s)")
    for c in row.get("components") or []:
        if str(c.get("status", "ok")) in ("down", "degraded", "error", "failed"):
            reasons.append(f"component {c.get('name')}: {c.get('status')}")
    m = row.get("metrics") or {}
    try:
        if m.get("disk_total_gb") and m.get("disk_free_gb") is not None and m["disk_free_gb"] / m["disk_total_gb"] < 0.05:
            reasons.append("disk almost full")
        if m.get("mem_total_mb") and m.get("mem_available_mb") is not None and m["mem_available_mb"] / m["mem_total_mb"] < 0.05:
            reasons.append("memory almost exhausted")
    except (TypeError, ZeroDivisionError):
        pass
    return (WorkerState.DEGRADED.value if reasons else WorkerState.ONLINE.value), reasons


def summarize_metrics(m: dict[str, Any] | None) -> dict[str, Any]:
    """Heartbeat metrics -> the compact percentages shown in the topology/UI (all keys always present)."""
    m = m or {}

    def pct(used_total: tuple[Any, Any]) -> float | None:
        total, avail = used_total
        try:
            return round((float(total) - float(avail)) / float(total) * 100.0, 1) if total else None
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    return {
        "cpu_percent": m.get("cpu_percent"), "mem_percent": pct((m.get("mem_total_mb"), m.get("mem_available_mb"))),
        "disk_percent": pct((m.get("disk_total_gb"), m.get("disk_free_gb"))), "load1": m.get("load1"),
        "net_rx_bps": m.get("net_rx_bps", 0), "net_tx_bps": m.get("net_tx_bps", 0),
        "mem_total_mb": m.get("mem_total_mb"), "mem_available_mb": m.get("mem_available_mb"),
    }


def public_worker(row: dict[str, Any]) -> dict[str, Any]:
    """Worker representation for the API (no token ids, certificate serials or internal bookkeeping)."""
    inv = row.get("inventory") or {}
    return {
        "id": row["id"], "name": row["name"], "hostname": row.get("hostname"), "address": row.get("address"), "state": row["state"],
        "scheduling": row["scheduling"], "pool": row["pool"], "labels": row.get("labels") or {}, "embedded": bool(row.get("embedded")),
        "agent_version": row.get("agent_version"), "protocol": row.get("protocol"), "os": row.get("os"), "arch": row.get("arch"),
        "connected": bool(row.get("connected")), "resources": {"cores": inv.get("cores", 0), "memory_mb": inv.get("memory_mb", 0), "disk_gb": inv.get("disk_gb", 0)},
        "inventory": {k: v for k, v in inv.items() if k not in ("env", "secrets")},
        "runtimes": row.get("runtimes") or [], "components": row.get("components") or [], "notes": row.get("notes") or {}, "metrics": row.get("metrics") or {},
        "enrolled_at": row.get("enrolled_at"), "last_seen_at": row.get("last_seen_at"), "last_connected_at": row.get("last_connected_at"),
        "last_disconnected_at": row.get("last_disconnected_at"), "revoked_at": row.get("revoked_at"), "etag": row["etag"],
        "cert_expires_at": row.get("cert_expires_at"), "token_expires_at": row.get("token_expires_at"),
    }


def _reserve_cores_validator(value: Any) -> int:
    """``compute.reserve_cores`` must be a whole number between 0 and 64 (anything else would make a node unschedulable or silently wrong)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise BadRequest("compute.reserve_cores must be an integer between 0 and 64")
    try:
        n = int(value)
        if isinstance(value, float) and value != n:
            raise ValueError
    except ValueError:
        raise BadRequest("compute.reserve_cores must be an integer between 0 and 64") from None
    if not 0 <= n <= 64:
        raise BadRequest("compute.reserve_cores must be an integer between 0 and 64")
    return n


class LockAborted(TimeoutError):
    """:meth:`ComputeRegistry.db_lock` stopped waiting because its ``cancel`` callback returned a reason."""


class ComputeRegistry:
    """Repository for the small configuration-like tables of the compute fabric."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.db = ctx.db
        self._reserve_cache: tuple[float, int] | None = None

    # ================================================================== cross-replica mutex
    @contextmanager
    def db_lock(self, name: str, *, ttl: float = 120.0, wait: float = 120.0, cancel: Callable[[], str | None] | None = None) -> Iterator[None]:
        """Mutual exclusion across API replicas (and threads) using a ``kv`` row with an expiry (portable SQLite/PostgreSQL).

        ``cancel`` is polled while waiting for the lock; when it returns a reason the wait ends with :class:`LockAborted` (a waiter whose job was cancelled, or
        whose provider was closed, must not keep polling for the full ``wait``).
        """
        key = f"lock.compute.{name}"[:191]
        holder = f"{self.ctx.node_id}:{threading.get_ident()}"
        deadline = time.monotonic() + wait
        local = self._local_lock(key)
        while not local.acquire(timeout=min(0.25, max(0.0, deadline - time.monotonic())) if cancel else wait):
            if cancel is not None and (reason := cancel()):
                raise LockAborted(f"gave up waiting for {name}: {reason}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"could not lock {name}")
        try:
            while True:
                now = now_iso()
                try:
                    with self.db.begin() as conn:
                        conn.execute(S.kv.insert().values(key=key, value={"holder": holder, "expires_at": iso_in(ttl)}, updated_at=now))
                    break
                except IntegrityError:
                    with self.db.connect() as conn:
                        cur = fetch_one(conn, select(S.kv).where(S.kv.c.key == key))
                    if cur and (cur["value"] or {}).get("expires_at", "") < now:
                        with self.db.begin() as conn:  # steal an expired lock (compare-and-set on updated_at)
                            if conn.execute(update(S.kv).where(S.kv.c.key == key, S.kv.c.updated_at == cur["updated_at"]).values(
                                    value={"holder": holder, "expires_at": iso_in(ttl)}, updated_at=now_iso())).rowcount:
                                break
                    if cancel is not None and (reason := cancel()):
                        raise LockAborted(f"gave up waiting for {name}: {reason}") from None
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"could not lock {name}") from None
                    time.sleep(0.1)
            yield
        finally:
            try:
                with self.db.begin() as conn:
                    cur = fetch_one(conn, select(S.kv).where(S.kv.c.key == key))
                    if cur and (cur["value"] or {}).get("holder") == holder:
                        conn.execute(S.kv.delete().where(S.kv.c.key == key))
            finally:
                local.release()

    #: In-process locks, per database OBJECT (weak keys): a registry built on another ``Database`` - another stack, another test - can never share a lock
    #: with this one, not even when Python reuses the ``id()`` of a collected database (the old ``id(self.db)`` key did, and a stale thread of the earlier
    #: stack then held the lock of the new one).  Registries on the same database still share the locks, so threads queue in memory before polling the row.
    _locks: weakref.WeakKeyDictionary[Any, dict[str, threading.Lock]] = weakref.WeakKeyDictionary()
    _locks_guard = threading.Lock()

    def _local_lock(self, key: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(self.db, {}).setdefault(key, threading.Lock())

    # ================================================================== settings
    @property
    def settings(self) -> Settings:
        return self.ctx.settings

    def setting(self, key: str, default: Any = None) -> Any:
        svc = self.ctx.maybe_service("settings")
        if svc is None:
            return default
        try:
            val = svc.get(key)
        except Exception:  # noqa: BLE001
            return default
        return default if val is None else val

    #: How long a read of ``compute.reserve_cores`` may be served from memory. Short, so an admin change takes effect within seconds on every node
    #: (the scheduler loop and the placement code call this for every node of every pass).
    RESERVE_CORES_CACHE_SECONDS = 2.0

    @property
    def reserve_cores(self) -> int:
        """Cores reserved per node for the OS and the agent: the live admin setting ``compute.reserve_cores`` (falls back to the startup configuration)."""
        now = time.monotonic()
        cached = self._reserve_cache
        if cached is not None and now - cached[0] < self.RESERVE_CORES_CACHE_SECONDS:
            return cached[1]
        raw = self.setting("compute.reserve_cores", self.settings.compute.reserve_cores)
        try:
            value = max(0, int(raw))
        except (TypeError, ValueError):
            value = max(0, int(self.settings.compute.reserve_cores))
        self._reserve_cache = (now, value)
        return value

    def register_settings(self) -> None:
        svc = self.ctx.maybe_service("settings")
        if svc is None:
            return
        c = self.settings.compute
        for key, desc, default in (
            ("compute.default_family", "Runtime family used when a job does not request one", "fabric-2.0-compatible"),
            ("compute.default_provider", "auto | standalone | local | <provider name>: where Spark jobs run by default", "auto"),
            ("compute.worker_lost_grace_seconds", "How long a job on a vanished worker waits for the worker to come back before it is failed", 180),
            ("compute.session_idle_timeout_seconds", "Idle notebook sessions are culled after this many seconds", 3600),
            ("compute.sql_session_idle_timeout_seconds", "Idle hidden SQL sessions are culled after this many seconds", 900),
            ("compute.starvation_seconds", "A resource-blocked job older than this closes its queue to backfilling", 300),
            ("compute.aging_interval_seconds", "Seconds of waiting that add one priority point", 60),
            ("compute.placement_strategy", "spread | pack: how drivers are spread over eligible nodes", "spread"),
            ("compute.driver_reserve_fraction", "Share of usable node resources kept free for drivers/kernels next to Spark workers", 0.25),
            ("compute.cancel_grace_seconds", "Grace period between cancel_job and a forced kill", 15),
            ("compute.reserve_cores", "Cores reserved per node for the OS and the agent (live: applies to the next scheduling pass)", c.reserve_cores),
            ("compute.allow_unsigned_runtimes", "Allow registering runtime manifests without a trusted signature (development only)", False),
            ("compute.runtime_install_timeout_seconds", "Per-node timeout of runtime install/repair/verify operations", 3600),
            ("compute.auto_install_runtimes", "Install a missing runtime automatically when a queued job waits for it", False),
            ("compute.host_python_fallback", "Python-engine notebooks, jobs and sessions run on the worker's own Python (runtime olcf-host-python, no Spark) when no installed runtime fits", True),
            ("compute.api_url", "Base URL workers use to call the control plane from inside jobs (empty = server.public_url / listen address)", ""),
            ("compute.provision_timeout_seconds", "A job the agent accepted must report 'running' within this time", 300),
            ("compute.missing_report_grace_seconds", "A running job that fresh heartbeats no longer list is failed after this long", 30),
            ("compute.stuck_state_seconds", "accepted/validating/provisioning states older than this are repaired after a restart", 300),
            ("compute.dispatch_timeout_seconds", "Timeout for handing a job to a provider/agent", 60),
            ("compute.infeasible_grace_seconds", "Queued jobs whose request can never fit the fleet are failed after this long", 300),
            ("compute.session_provider", "local | standalone | auto: where interactive sessions run their Spark driver (local = in the kernel's node)", "local"),
            ("compute.max_sessions_per_user", "Maximum concurrent notebook sessions per user", 5),
            ("compute.session_start_timeout_seconds", "Time an agent may take to start a kernel", 180),
            ("compute.session_replay_events", "Output events kept per session for browser reconnects", 500),
            ("compute.max_artifact_bytes", "Hard cap for a single uploaded job artifact", 2147483648),
            ("compute.remote_status_error_limit", "Consecutive failed status polls before a remote-provider job is declared lost", 30),
            ("compute.remote_log_pages_per_poll", "Log pages (256 KiB each) a remote-provider job ingests per monitor poll; more is ingested on the following polls", 16),
            ("compute.remote_drain_timeout_seconds", "After a remote application ended, the time the monitor keeps reading its remaining output before it finishes the job (a visible log gap is recorded if it ran out)", 300),
            ("compute.worker_cert_renew_before_days", "Worker machine certificates are renewed over the control channel when fewer than this many days remain (never wider than half the lifetime)", 30),
            ("compute.worker_cert_lifetime_days", "Validity of the machine certificates the controller signs for workers (enrolment and renewal)", 90),
            ("compute.worker_cert_auto_renew", "Agents renew their certificate / token on their own inside the renewal window; off = only `olcf worker renew` renews", True),
            ("compute.worker_token_max_age_days", "Worker tokens expire after this many days and are rotated inside the renewal exchange; 0 = tokens never expire (default, unchanged behaviour)", 0),
            ("compute.worker_token_grace_seconds", "A superseded worker token / certificate keeps working this long, or until the new ones are first used", 86400),
            ("compute.remote_result_grace_seconds", "After a remote application ended without delivering its result, the time spent reconciling (re-reading the retained output) before the job is failed as 'result missing'", 120),
        ):
            try:
                svc.register(key, description=desc, default=default, validator=_reserve_cores_validator if key == "compute.reserve_cores" else None)
            except Exception:  # noqa: BLE001
                log.debug("setting %s already registered", key)

    # ================================================================== workers
    def get_worker(self, worker_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.workers).where(C.workers.c.id == worker_id))
        if not row:
            raise NotFound(f"worker {worker_id} not found")
        return row

    def find_worker(self, ref: str) -> dict[str, Any]:
        """Resolve a worker by id or by (unique) name."""
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.workers).where((C.workers.c.id == ref) | (C.workers.c.name == ref)))
        if not row:
            raise NotFound(f"worker '{ref}' not found")
        return row

    def list_workers(self, *, state: str | None = None, pool: str | None = None, include_revoked: bool = True) -> list[dict[str, Any]]:
        stmt = select(C.workers).order_by(C.workers.c.name)
        if state:
            stmt = stmt.where(C.workers.c.state == state)
        if pool:
            stmt = stmt.where(C.workers.c.pool == pool)
        if not include_revoked:
            stmt = stmt.where(C.workers.c.revoked_at.is_(None))
        with self.db.connect() as conn:
            return fetch_all(conn, stmt)

    def update_worker(self, worker_id: str, **values: Any) -> dict[str, Any]:
        values.setdefault("updated_at", now_iso())
        if any(k in values for k in ("labels", "pool", "scheduling", "state", "notes")):
            values.setdefault("etag", new_etag())
        with self.db.begin() as conn:
            n = conn.execute(update(C.workers).where(C.workers.c.id == worker_id).values(**values)).rowcount
            if not n:
                raise NotFound(f"worker {worker_id} not found")
        return self.get_worker(worker_id)

    def set_worker_state(self, worker_id: str, new_state: str, *, reason: str = "", only_from: tuple[str, ...] | None = None) -> bool:
        """Compare-and-set state change; publishes ``worker.<state>`` on a real change. Returns True if it changed."""
        w = C.workers.c
        with self.db.begin() as conn:
            cur = fetch_one(conn, select(w.state, w.name, w.pool).where(w.id == worker_id))
            if not cur or cur["state"] == new_state:
                return False
            if only_from is not None and cur["state"] not in only_from:
                return False
            if cur["state"] == WorkerState.REVOKED.value:
                return False
            conn.execute(update(C.workers).where(w.id == worker_id).values(state=new_state, updated_at=now_iso(), etag=new_etag()))
        self.ctx.events.publish(f"worker.{new_state}", {"worker_id": worker_id, "name": cur["name"], "from": cur["state"], "reason": reason, "pool": cur["pool"]}, subject=worker_id)
        return True

    def node_infos(self, *, include_revoked: bool = False) -> list[NodeInfo]:
        return [NodeInfo.from_row(r) for r in self.list_workers(include_revoked=include_revoked)]

    def node_views(self, allocations: dict[str, tuple[int, int]], ports: dict[str, set[int]], *, sharing: dict[str, int] | None = None) -> list[NodeView]:
        """Planner views for every non-revoked worker; ``allocations`` maps node id -> (cores, memory_mb) already promised."""
        c = self.settings.compute
        out: list[NodeView] = []
        now = time.time()
        for n in self.node_infos():
            m = n.row.get("metrics") or {}
            fresh = n.row.get("last_seen_at") and (now - from_iso(n.row["last_seen_at"]).timestamp()) < max(3 * c.worker_heartbeat_seconds, 10)
            live = int(m["mem_available_mb"]) if fresh and m.get("mem_available_mb") is not None else None
            cores_a, mem_a = allocations.get(n.id, (0, 0))
            out.append(NodeView(
                id=n.id, name=n.name, pool=n.pool, labels=n.labels, state=n.state, scheduling=n.scheduling, cores_total=n.sched_cores,
                memory_mb_total=n.sched_memory_mb, reserve_cores=self.reserve_cores, reserve_memory_fraction=c.reserve_memory_fraction,
                cores_allocated=cores_a, memory_allocated_mb=mem_a, live_available_memory_mb=live, runtimes_ready=n.ready_runtimes,
                ports_used=frozenset(ports.get(n.id, set())),
            ))
        return out

    # ================================================================== pools
    def ensure_default_pool(self) -> None:
        with self.db.begin() as conn:
            if fetch_one(conn, select(C.worker_pools.c.id).where(C.worker_pools.c.name == DEFAULT_POOL)):
                return
            now = now_iso()
            insert_row(conn, C.worker_pools, {"id": new_id("pol"), "name": DEFAULT_POOL, "description": "Default worker pool", "reserved": False,
                                              "workspace_ids": [], "labels": {}, "etag": new_etag(), "created_at": now, "updated_at": now})

    def list_pools(self) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            pools = fetch_all(conn, select(C.worker_pools).order_by(C.worker_pools.c.name))
            counts = dict(conn.execute(select(C.workers.c.pool, func.count()).where(C.workers.c.revoked_at.is_(None)).group_by(C.workers.c.pool)).all())
        for p in pools:
            p["workers"] = int(counts.get(p["name"], 0))
        return pools

    def get_pool(self, ref: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.worker_pools).where((C.worker_pools.c.id == ref) | (C.worker_pools.c.name == ref)))
        if not row:
            raise NotFound(f"worker pool '{ref}' not found")
        return row

    def pool_exists(self, name: str) -> bool:
        with self.db.connect() as conn:
            return fetch_one(conn, select(C.worker_pools.c.id).where(C.worker_pools.c.name == name)) is not None

    def create_pool(self, name: str, *, description: str = "", reserved: bool = False, workspace_ids: list[str] | None = None,
                    labels: dict[str, str] | None = None) -> dict[str, Any]:
        if not name or len(name) > 64 or not all(ch.isalnum() or ch in "-_." for ch in name):
            raise BadRequest("pool name must be 1-64 characters of letters, digits, '-', '_' or '.'")
        if self.pool_exists(name):
            raise Conflict(f"worker pool '{name}' already exists")
        now = now_iso()
        row = {"id": new_id("pol"), "name": name, "description": description, "reserved": reserved, "workspace_ids": workspace_ids or [],
               "labels": labels or {}, "etag": new_etag(), "created_at": now, "updated_at": now}
        with self.db.begin() as conn:
            insert_row(conn, C.worker_pools, row)
        return row

    def update_pool(self, ref: str, changes: dict[str, Any]) -> dict[str, Any]:
        pool = self.get_pool(ref)
        vals = {k: v for k, v in changes.items() if k in ("description", "reserved", "workspace_ids", "labels") and v is not None}
        if vals:
            vals.update(updated_at=now_iso(), etag=new_etag())
            with self.db.begin() as conn:
                conn.execute(update(C.worker_pools).where(C.worker_pools.c.id == pool["id"]).values(**vals))
        return self.get_pool(pool["id"])

    def delete_pool(self, ref: str) -> dict[str, Any]:
        pool = self.get_pool(ref)
        if pool["name"] == DEFAULT_POOL:
            raise Conflict("the default pool cannot be deleted")
        with self.db.begin() as conn:
            n = conn.execute(select(func.count()).select_from(C.workers).where(C.workers.c.pool == pool["name"], C.workers.c.revoked_at.is_(None))).scalar_one()
            if n:
                raise Conflict(f"pool '{pool['name']}' still has {n} worker(s); move them first")
            conn.execute(C.worker_pools.delete().where(C.worker_pools.c.id == pool["id"]))
        return pool

    # ================================================================== queues
    def ensure_default_queue(self) -> None:
        with self.db.begin() as conn:
            if fetch_one(conn, select(C.queues.c.id).where(C.queues.c.name == DEFAULT_QUEUE)):
                return
            now = now_iso()
            insert_row(conn, C.queues, {"id": new_id("que"), "name": DEFAULT_QUEUE, "description": "Default FIFO/priority queue", "max_concurrency": None,
                                        "max_cores": None, "max_memory_mb": None, "max_queued": None, "max_queue_wait_seconds": None,
                                        "workspace_limits": {}, "fair_share_weights": {}, "allowed_pools": [], "default_priority": 50, "paused": False,
                                        "etag": new_etag(), "created_at": now, "updated_at": now})

    def list_queues(self) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return fetch_all(conn, select(C.queues).order_by(C.queues.c.name))

    def get_queue(self, ref: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.queues).where((C.queues.c.id == ref) | (C.queues.c.name == ref)))
        if not row:
            raise NotFound(f"queue '{ref}' not found")
        return row

    def queue_exists(self, name: str) -> bool:
        with self.db.connect() as conn:
            return fetch_one(conn, select(C.queues.c.id).where(C.queues.c.name == name)) is not None

    QUEUE_FIELDS = ("description", "max_concurrency", "max_cores", "max_memory_mb", "max_queued", "max_queue_wait_seconds", "workspace_limits",
                    "fair_share_weights", "allowed_pools", "default_priority", "paused")

    def create_queue(self, name: str, fields: dict[str, Any]) -> dict[str, Any]:
        if not name or len(name) > 64 or not all(ch.isalnum() or ch in "-_." for ch in name):
            raise BadRequest("queue name must be 1-64 characters of letters, digits, '-', '_' or '.'")
        if self.queue_exists(name):
            raise Conflict(f"queue '{name}' already exists")
        now = now_iso()
        row = {"id": new_id("que"), "name": name, "description": "", "max_concurrency": None, "max_cores": None, "max_memory_mb": None, "max_queued": None,
               "max_queue_wait_seconds": None, "workspace_limits": {}, "fair_share_weights": {}, "allowed_pools": [], "default_priority": 50,
               "paused": False, "etag": new_etag(), "created_at": now, "updated_at": now}
        row.update({k: v for k, v in fields.items() if k in self.QUEUE_FIELDS})
        for p in row["allowed_pools"] or []:
            if not self.pool_exists(p):
                raise BadRequest(f"unknown pool '{p}' in allowed_pools")
        with self.db.begin() as conn:
            insert_row(conn, C.queues, row)
        return row

    def update_queue(self, ref: str, changes: dict[str, Any], *, fields_set: set[str]) -> dict[str, Any]:
        q = self.get_queue(ref)
        vals = {k: v for k, v in changes.items() if k in self.QUEUE_FIELDS and k in fields_set}
        for p in vals.get("allowed_pools") or []:
            if not self.pool_exists(p):
                raise BadRequest(f"unknown pool '{p}' in allowed_pools")
        if vals:
            vals.update(updated_at=now_iso(), etag=new_etag())
            with self.db.begin() as conn:
                conn.execute(update(C.queues).where(C.queues.c.id == q["id"]).values(**vals))
        return self.get_queue(q["id"])

    def delete_queue(self, ref: str) -> dict[str, Any]:
        q = self.get_queue(ref)
        if q["name"] == DEFAULT_QUEUE:
            raise Conflict("the default queue cannot be deleted")
        with self.db.begin() as conn:
            n = conn.execute(select(func.count()).select_from(C.jobs).where(C.jobs.c.queue == q["name"], C.jobs.c.state.in_(("accepted", "validating", "queued", "provisioning", "running")))).scalar_one()
            if n:
                raise Conflict(f"queue '{q['name']}' still has {n} unfinished job(s)")
            conn.execute(C.queues.delete().where(C.queues.c.id == q["id"]))
        return q

    # ================================================================== providers
    def list_providers(self) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return fetch_all(conn, select(C.providers).order_by(C.providers.c.name))

    def get_provider_row(self, ref: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(C.providers).where((C.providers.c.id == ref) | (C.providers.c.name == ref)))
        if not row:
            raise NotFound(f"provider '{ref}' not found")
        return row

    def ensure_builtin_providers(self) -> None:
        now = now_iso()
        with self.db.begin() as conn:
            have = {r[0] for r in conn.execute(select(C.providers.c.name)).all()}
            for name in ("standalone", "local"):
                if name not in have:
                    insert_row(conn, C.providers, {"id": new_id("prv"), "name": name, "type": name, "config": {}, "secret_id": None, "enabled": True,
                                                   "is_default": False, "last_discovery": None, "etag": new_etag(), "created_at": now, "updated_at": now})

    def save_provider(self, row: dict[str, Any]) -> None:
        with self.db.begin() as conn:
            conn.execute(update(C.providers).where(C.providers.c.id == row["id"]).values(**{k: v for k, v in row.items() if k != "id"}))


def worker_allocation_summary(rows_jobs: list[dict[str, Any]], rows_sessions: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> tuple[dict[str, tuple[int, int]], dict[str, set[int]]]:
    """Promised (cores, memory_mb) and used driver ports per node from running jobs, active sessions and Spark daemon reservations."""
    alloc: dict[str, list[int]] = {}
    ports: dict[str, set[int]] = {}

    def add(node: str | None, cores: int, mem: int) -> None:
        if not node:
            return
        a = alloc.setdefault(node, [0, 0])
        a[0] += int(cores or 0)
        a[1] += int(mem or 0)

    for j in rows_jobs:
        g = j.get("resources_granted") or {}
        add(j.get("driver_worker_id"), g.get("driver_cores", 0), g.get("driver_memory_mb", 0))
        for k in ("driver_port", "block_manager_port"):
            if g.get(k) and j.get("driver_worker_id"):
                ports.setdefault(j["driver_worker_id"], set()).add(int(g[k]))
    for s in rows_sessions:
        r = s.get("resources") or {}
        add(s.get("worker_id"), r.get("driver_cores", r.get("cores", 0)), r.get("driver_memory_mb", r.get("memory_mb", 0)))
        for k in ("driver_port", "block_manager_port"):
            if r.get(k) and s.get("worker_id"):
                ports.setdefault(s["worker_id"], set()).add(int(r[k]))
    for cl in clusters:
        for wid, w in (cl.get("spark_workers") or {}).items():
            if w.get("state") in ("starting", "running"):
                add(wid, w.get("cores", 0), w.get("memory_mb", 0))
    return {k: (v[0], v[1]) for k, v in alloc.items()}, ports


__all__ = [
    "ACTIVE_JOB_STATES", "ACTIVE_SESSION_STATES", "ComputeRegistry", "DEFAULT_POOL", "DEFAULT_QUEUE", "HISTORY_SAMPLES", "NodeInfo",
    "SchedulingState", "assess_health", "public_worker", "summarize_metrics", "worker_allocation_summary",
]
