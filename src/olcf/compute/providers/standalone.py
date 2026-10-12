# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark Standalone provider: one real Spark cluster per *runtime x pool*, managed across enrolled agents.

How a cluster comes to life (``ensure_cluster``)
------------------------------------------------
1. Eligible nodes = connected, schedulable, in the pool, with the runtime ``ready``.
2. One node becomes the **master** (sticky: an existing healthy master stays); the controller sends ``start_spark_master``
   with a free RPC/UI port on that node and the node's *advertise host*.
3. Every eligible node gets a **Spark worker** (``start_spark_worker``) sized from the node's usable resources
   (``compute.reserve_*`` headroom, minus a driver share, divided between the runtimes that run clusters on the node), so
   daemons + drivers can never oversubscribe the machine. Workers always point at *this* cluster's master; runtimes are
   never mixed in one cluster.
4. The controller waits until the master's ``/json`` reports the workers ``ALIVE``.

Jobs then run their driver (the OLCF runner) on a node chosen by the scheduler and connect to ``spark://master``;
``prepare`` produces the exact ``spark`` block (cores cap, executor sizing, driver host and fixed ports, dynamic allocation).
Live cluster state comes from the master's ``/json`` endpoint (cached in ``clusters.last_status`` by ``maintain``).
Cordoned nodes keep their daemons but never receive new ones; offline nodes are marked ``stopped`` in the cluster record.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
from sqlalchemy import select, update

from olcf.compute import schema as C
from olcf.compute.errors import ProviderError, WorkerCommandFailed
from olcf.compute.planner import NodeView, standalone_worker_sizing
from olcf.compute.providers.agent import AgentExecutedProvider
from olcf.compute.providers.base import (
    AppStatus,
    Discovery,
    PrepareRequest,
    base_spark_conf,
    dynamic_allocation_conf,
    spark_memory,
)
from olcf.compute.registry import ACTIVE_JOB_STATES, ACTIVE_SESSION_STATES, LockAborted, NodeInfo
from olcf.core.errors import OlcfError
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, now_iso
from olcf.db.util import fetch_all, fetch_one, insert_row, new_etag

log = logging.getLogger("olcf.compute.standalone")

_APP_NAME_RE = re.compile(r"olcf-((?:job|ses)_[0-9A-Za-z]{10,40})")
_MASTER_APP_STATES = {"RUNNING": "RUNNING", "WAITING": "WAITING", "FINISHED": "FINISHED", "KILLED": "KILLED", "FAILED": "FAILED"}


def parse_master_json(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise the Spark master ``/json`` document (``/json/`` of the standalone master web UI)."""
    workers = []
    for w in raw.get("workers") or []:
        workers.append({
            "id": w.get("id"), "host": w.get("host"), "port": w.get("port"), "state": w.get("state"), "cores": w.get("cores", 0), "cores_used": w.get("coresused", 0),
            "memory_mb": w.get("memory", 0), "memory_used_mb": w.get("memoryused", 0), "webui": w.get("webuiaddress"),
        })

    def app(a: dict[str, Any]) -> dict[str, Any]:
        name = a.get("name") or ""
        m = _APP_NAME_RE.search(name)
        return {"id": a.get("id"), "name": name, "cores": a.get("cores", 0), "state": a.get("state", "UNKNOWN"), "memory_per_executor_mb": a.get("memoryperexecutor") or a.get("memoryperslave"),
                "duration_ms": a.get("duration"), "submit_date": a.get("submitdate"), "start_time": a.get("starttime"), "user": a.get("user"), "ref": m.group(1) if m else None}

    return {
        "url": raw.get("url"), "state": raw.get("status", "UNKNOWN"), "alive_workers": int(raw.get("aliveworkers", sum(1 for w in workers if w["state"] == "ALIVE"))),
        "cores_total": raw.get("cores", 0), "cores_used": raw.get("coresused", 0), "memory_total_mb": raw.get("memory", 0), "memory_used_mb": raw.get("memoryused", 0),
        "workers": workers, "applications": [app(a) for a in (raw.get("activeapps") or [])], "completed_applications": [app(a) for a in (raw.get("completedapps") or [])[-50:]],
    }


Cancelled = Callable[[], "str | None"]  # returns the reason to stop waiting, else None


class LaunchAborted(ProviderError):
    """A cluster start stopped waiting because the job that asked for it was cancelled or the provider was closed."""
    code = "launch_aborted"


class StandaloneProvider(AgentExecutedProvider):
    """Runs jobs on Spark Standalone clusters that OLCF itself starts across enrolled workers, one master per runtime and pool."""
    type = "standalone"
    capabilities = ("spark-standalone", "multi-node", "dynamic-allocation", "cordon-drain", "reconcile")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # cluster-internal traffic (master UI/REST on the nodes' own addresses) never goes through HTTP(S)_PROXY: a site proxy cannot reach those addresses
        # and would make a healthy cluster look dead
        self._http = httpx.Client(timeout=httpx.Timeout(3.0, connect=2.0), follow_redirects=False, trust_env=False)
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="olcf-standalone")
        self._guard = threading.Lock()
        self._closed = threading.Event()  # set by close(): every wait loop of a launch ends at once instead of polling a dead master for its whole timeout

    def _stop_reason(self, cancelled: Cancelled | None = None) -> str | None:
        """Why a launch must stop waiting: the provider was closed, or the caller's ``cancelled`` callback gave a reason."""
        if self._closed.is_set():
            return "the compute provider was closed"
        return cancelled() if cancelled is not None else None

    def _check(self, cancelled: Cancelled | None, what: str) -> None:
        reason = self._stop_reason(cancelled)
        if reason:
            raise LaunchAborted(f"{what} stopped: {reason}")

    def _job_cancelled(self, job_id: str | None) -> Cancelled | None:
        """Callback for ``job_id``: a launch only makes sense while the job is still provisioning; any other state (cancelled, failed, finished) ends the wait."""
        if not job_id:
            return None

        def check() -> str | None:
            with self.env.ctx.db.connect() as conn:
                state = conn.execute(select(C.jobs.c.state).where(C.jobs.c.id == job_id)).scalar()
            return None if state == "provisioning" else f"job {job_id} is {state or 'gone'}"

        return check

    # ================================================================== configuration
    @property
    def driver_reserve_fraction(self) -> float:
        return float(self.config.get("driver_reserve_fraction", self.env.registry.setting("compute.driver_reserve_fraction", 0.25)))

    @property
    def start_timeout(self) -> float:
        return float(self.config.get("start_timeout_seconds", 90))

    def _nodeview(self, n: NodeInfo) -> NodeView:
        c = self.env.ctx.settings.compute
        return NodeView(id=n.id, name=n.name, pool=n.pool, labels=n.labels, state=n.state, scheduling=n.scheduling, cores_total=n.sched_cores, memory_mb_total=n.sched_memory_mb,
                        reserve_cores=self.env.registry.reserve_cores, reserve_memory_fraction=c.reserve_memory_fraction)

    # ================================================================== cluster rows
    def clusters(self, *, runtime_id: str | None = None, pool: str | None = None) -> list[dict[str, Any]]:
        stmt = select(C.clusters).where(C.clusters.c.provider == self.name).order_by(C.clusters.c.created_at)
        if runtime_id:
            stmt = stmt.where(C.clusters.c.runtime_id == runtime_id)
        if pool:
            stmt = stmt.where(C.clusters.c.pool == pool)
        with self.env.ctx.db.connect() as conn:
            return fetch_all(conn, stmt)

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        with self.env.ctx.db.connect() as conn:
            return fetch_one(conn, select(C.clusters).where(C.clusters.c.id == cluster_id))

    def _save(self, cluster_id: str, **vals: Any) -> dict[str, Any]:
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.env.ctx.db.begin() as conn:
            conn.execute(update(C.clusters).where(C.clusters.c.id == cluster_id).values(**vals))
        return self.get_cluster(cluster_id)  # type: ignore[return-value]

    def _set_state(self, cl: dict[str, Any], state: str, detail: str = "") -> dict[str, Any]:
        # the detail always describes the CURRENT state: an empty detail clears a stale one ("0/? workers alive" must not outlive the workers registering)
        if cl["state"] == state and (cl.get("detail") or "") == detail:
            return cl
        new = self._save(cl["id"], state=state, detail=detail)
        if cl["state"] != state:
            self.env.ctx.events.publish("cluster.state_changed", {"cluster_id": cl["id"], "runtime_id": cl["runtime_id"], "pool": cl["pool"], "provider": self.name,
                                                                  "from": cl["state"], "to": state, "detail": detail}, subject=cl["id"])
        return new

    def _create_cluster(self, runtime_id: str, pool: str) -> dict[str, Any]:
        rt = self._runtime_row(runtime_id)
        now = now_iso()
        row = {"id": new_id("clu"), "provider": self.name, "runtime_id": runtime_id, "pool": pool, "family": rt.get("family") if rt else None, "state": "stopped",
               "master_worker_id": None, "master_host": None, "master_port": None, "ui_port": None, "master_url": None, "ui_url": None, "spark_workers": {},
               "last_status": {}, "last_status_at": None, "detail": "", "etag": new_etag(), "created_at": now, "updated_at": now}
        with self.env.ctx.db.begin() as conn:
            insert_row(conn, C.clusters, row)
        return row

    def _runtime_row(self, runtime_id: str) -> dict[str, Any] | None:
        with self.env.ctx.db.connect() as conn:
            return fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == runtime_id))

    # ================================================================== node selection
    def _effective_connected(self, n: NodeInfo) -> bool:
        if not n.connected or n.state not in ("online", "degraded"):
            return False
        seen = n.row.get("last_seen_at")
        return bool(seen and time.time() - from_iso(seen).timestamp() < self.env.ctx.settings.compute.worker_offline_after_seconds)

    def eligible_nodes(self, runtime_id: str, pool: str, *, include_cordoned: bool = False) -> list[NodeInfo]:
        out = []
        for n in self.env.registry.node_infos():
            if n.pool != pool or not self._effective_connected(n) or runtime_id not in n.ready_runtimes:
                continue
            if n.scheduling != "active" and not include_cordoned:
                continue
            out.append(n)
        return sorted(out, key=lambda n: n.id)

    def _runtimes_sharing(self, pool: str, runtime_id: str) -> int:
        rts = {c["runtime_id"] for c in self.clusters(pool=pool) if c["state"] != "stopped"}
        with self.env.ctx.db.connect() as conn:  # runtimes that are about to need a cluster too (queued/starting Spark jobs): size for them now, so none has to be shrunk later
            rts |= {r[0] for r in conn.execute(select(C.jobs.c.runtime_id).where(C.jobs.c.engine == "spark", C.jobs.c.runtime_id.is_not(None),
                                                                                  C.jobs.c.state.in_(("queued", "provisioning", "running"))).distinct())}
        return max(1, len(rts | {runtime_id}))

    def desired_workers(self, runtime_id: str, pool: str) -> dict[str, tuple[int, int]]:
        """node id -> (cores, memory_mb) the Spark worker of this cluster should advertise."""
        share = self._runtimes_sharing(pool, runtime_id)
        held: dict[str, tuple[int, int]] = {}  # what the *other* runtimes' daemons already occupy on each node (they may be busy and therefore not resizable right now)
        for other in self.clusters(pool=pool):
            if other["runtime_id"] == runtime_id or other["state"] == "stopped":
                continue
            for nid, w in (other.get("spark_workers") or {}).items():
                if w.get("state") in ("running", "starting"):
                    c0, m0 = held.get(nid, (0, 0))
                    held[nid] = (c0 + int(w.get("cores") or 0), m0 + int(w.get("memory_mb") or 0))
        out = {}
        for n in self.eligible_nodes(runtime_id, pool):
            nv = self._nodeview(n)
            cores, mem = standalone_worker_sizing(nv, share, self.driver_reserve_fraction)
            cap_c, cap_m = standalone_worker_sizing(nv, 1, self.driver_reserve_fraction)
            used_c, used_m = held.get(n.id, (0, 0))
            cores, mem = min(cores, cap_c - used_c), min(mem, cap_m - used_m)  # never oversubscribe the node, even while another runtime's worker is larger than its fair share
            if cores > 0 and mem >= 512:
                out[n.id] = (cores, mem)
        return out

    def _fair_workers(self, runtime_id: str, pool: str) -> dict[str, tuple[int, int]]:
        share = self._runtimes_sharing(pool, runtime_id)
        return {n.id: standalone_worker_sizing(self._nodeview(n), share, self.driver_reserve_fraction) for n in self.eligible_nodes(runtime_id, pool)}

    def _make_room(self, runtime_id: str, pool: str, wait: float, cancelled: Cancelled | None = None) -> dict[str, tuple[int, int]]:
        """``desired_workers`` for a cluster, shrinking idle clusters of other runtimes (or waiting for busy ones) when they hold the whole node.

        A node's Spark capacity is shared between the runtimes that run a cluster on it. A cluster created first sized itself for the node alone; when a second
        runtime needs a cluster, an *idle* neighbour is resized to its fair share (its daemon restarts) and a *busy* one is waited for (it cannot be shrunk without
        killing executors). The node is never oversubscribed.
        """
        deadline = time.monotonic() + wait
        while True:
            self._check(cancelled, "making room for the Spark cluster")
            desired = self.desired_workers(runtime_id, pool)
            if not self.eligible_nodes(runtime_id, pool):
                return desired
            fair = self._fair_workers(runtime_id, pool)
            if desired and all(nid in desired and desired[nid][0] >= fc for nid, (fc, _fm) in fair.items() if fc > 0):
                return desired
            shrunk = False
            for other in self.clusters(pool=pool):
                if other["runtime_id"] != runtime_id and other["state"] != "stopped" and not self._busy(other["id"]):
                    shrunk |= self._shrink_to_share(other, pool)
            if shrunk:
                continue
            if desired or time.monotonic() >= deadline:
                return desired  # partly short of the fair share: run with what is free (busy neighbours cannot be shrunk); nothing at all: wait for them

            self._closed.wait(0.5)

    def _shrink_to_share(self, cl: dict[str, Any], pool: str) -> bool:
        """Resize the idle cluster's workers down to the fair share of the node (the caller holds the pool lock). True when at least one worker was resized."""
        cl = self.get_cluster(cl["id"]) or cl
        if self._busy(cl["id"]):
            return False
        share = self._runtimes_sharing(pool, cl["runtime_id"])
        nodes = {n.id: n for n in self.eligible_nodes(cl["runtime_id"], pool, include_cordoned=True)}
        workers = dict(cl.get("spark_workers") or {})
        changed = False
        for nid, w in list(workers.items()):
            node = nodes.get(nid)
            if node is None or w.get("state") not in ("running", "starting"):
                continue
            cores, mem = standalone_worker_sizing(self._nodeview(node), share, self.driver_reserve_fraction)
            if cores > 0 and (int(w.get("cores") or 0) > cores or int(w.get("memory_mb") or 0) > mem):
                try:
                    workers[nid] = self._start_worker(cl, node, cores, mem, True)
                    changed = True
                except OlcfError as exc:
                    log.warning("could not shrink the Spark worker on %s: %s", node.name, exc.message)
        if changed:
            self._save(cl["id"], spark_workers=workers)
        return changed

    def _busy(self, cluster_id: str) -> bool:
        with self.env.ctx.db.connect() as conn:
            j = conn.execute(select(C.jobs.c.id).where(C.jobs.c.cluster_id == cluster_id, C.jobs.c.state.in_(ACTIVE_JOB_STATES)).limit(1)).first()
            s = conn.execute(select(C.sessions.c.id).where(C.sessions.c.cluster_id == cluster_id, C.sessions.c.state.in_(ACTIVE_SESSION_STATES)).limit(1)).first()
        return bool(j or s)

    # ================================================================== master status
    def master_status(self, cl: dict[str, Any]) -> dict[str, Any] | None:
        """Live state from the master's ``/json``: direct HTTP first, otherwise through the master's agent (``probe``)."""
        if not cl.get("ui_url"):
            return None
        try:
            r = self._http.get(cl["ui_url"].rstrip("/") + "/json/")
            if r.status_code == 200:
                return parse_master_json(r.json())
        except (httpx.HTTPError, ValueError):
            pass
        wid = cl.get("master_worker_id")
        if wid:
            try:  # the controller may not be able to reach the master UI port (firewall/NAT) - ask the node itself
                res = self.env.hub.request(wid, "probe", {"checks": [{"kind": "spark_master", "url": cl["ui_url"].rstrip("/") + "/json/", "name": "spark-master"}]}, timeout=8.0)
                for chk in (res or {}).get("checks", []):
                    if chk.get("kind") == "spark_master" and chk.get("ok") and isinstance(chk.get("data"), dict):
                        return parse_master_json(chk["data"])
            except OlcfError:
                pass
        return None

    def refresh_cluster(self, cl: dict[str, Any]) -> dict[str, Any]:
        """Poll the master, cache ``last_status``, derive the cluster state and sync the Spark application table."""
        if cl["state"] == "stopped" and not cl.get("master_url"):
            return cl
        st = self.master_status(cl)
        if st is None:
            return self._set_state(self._save(cl["id"], last_status={}, last_status_at=now_iso()), "down", "Spark master is not reachable")
        expected = sum(1 for w in (cl.get("spark_workers") or {}).values() if w.get("state") in ("running", "starting"))
        state = "up" if st["alive_workers"] >= max(1, expected) else "degraded"
        if state != "degraded":
            detail = ""
        elif expected:
            detail = f"{st['alive_workers']}/{expected} workers alive"
        else:
            detail = f"{st['alive_workers']} workers alive; none has been started by the controller yet"
        cl = self._save(cl["id"], last_status=st, last_status_at=now_iso())
        cl = self._set_state(cl, state, detail)
        self._sync_apps(cl, st)
        return cl

    # ================================================================== lifecycle
    def ensure_cluster(self, runtime_id: str, pool: str, *, wait: float | None = None, cancelled: Cancelled | None = None) -> dict[str, Any]:
        """Bring the (runtime, pool) cluster to a running state and return its row. Idempotent and safe across replicas.

        ``cancelled`` is polled by every wait (the pool lock, room for the workers, master start, worker registration); a reason ends the start with
        :class:`LaunchAborted`.  So does :meth:`close`.  Nothing here may keep polling a dead master after its caller is gone.
        """
        wait = self.start_timeout if wait is None else wait
        stop = lambda: self._stop_reason(cancelled)  # noqa: E731
        try:
            with self.env.registry.db_lock(f"cluster.{self.name}.{pool}", ttl=wait + 60, wait=wait + 60, cancel=stop):  # one lock per pool: sizing decisions of runtimes that share nodes must be serial
                return self._ensure_locked(runtime_id, pool, wait, cancelled)
        except LockAborted as exc:
            raise LaunchAborted(f"cluster start stopped: {exc}") from exc

    def _ensure_locked(self, runtime_id: str, pool: str, wait: float, cancelled: Cancelled | None = None) -> dict[str, Any]:
        self._check(cancelled, "cluster start")
        cl = (self.clusters(runtime_id=runtime_id, pool=pool) or [None])[0] or self._create_cluster(runtime_id, pool)
        desired = self._make_room(runtime_id, pool, min(wait, 60.0), cancelled)
        if not desired:
            raise ProviderError(f"no eligible node in pool '{pool}' can run Spark Standalone workers for runtime '{runtime_id}'",
                                remediation="install/verify the runtime on at least one online, uncordoned node with >= 2 usable cores")
        nodes = {n.id: n for n in self.eligible_nodes(runtime_id, pool, include_cordoned=True)}
        active = {nid for nid in desired}
        # ---- master: sticky while it is connected and still serving
        master_node = nodes.get(cl.get("master_worker_id") or "")
        status = self.master_status(cl) if (master_node and cl.get("ui_url")) else None
        if master_node is None or status is None:
            cl = self._start_master(cl, runtime_id, pool, [nodes[i] for i in sorted(active)] or list(nodes.values()), cancelled)
            for w in (cl.get("spark_workers") or {}).values():  # workers of the old master are useless
                w["state"] = "stopped"
            cl = self._save(cl["id"], spark_workers=cl["spark_workers"])
        # ---- workers
        if cl["state"] in ("stopped", "down"):
            cl = self._set_state(cl, "starting", "starting Spark workers")
        workers = dict(cl.get("spark_workers") or {})
        todo: list[tuple[str, int, int, bool]] = []
        for nid, (cores, mem) in desired.items():
            cur = workers.get(nid) or {}
            running = cur.get("state") in ("running", "starting") and cur.get("master_url") == cl["master_url"]
            if running and cur.get("cores") == cores and cur.get("memory_mb") == mem:
                continue
            if running and not self._busy(cl["id"]):
                todo.append((nid, cores, mem, True))  # resize while idle
            elif not running:
                # a stale daemon (pointing at a dead/old master) may still exist on the node: always stop before starting
                todo.append((nid, cores, mem, bool(cur)))
        for nid, w in list(workers.items()):
            if nid not in nodes and w.get("state") in ("running", "starting"):
                w["state"] = "stopped"  # node vanished; nothing to stop remotely
        futures = {nid: self._pool.submit(self._start_worker, cl, nodes[nid], cores, mem, restart) for nid, cores, mem, restart in todo}
        for nid, fut in futures.items():
            try:
                workers[nid] = fut.result(timeout=wait + 30)  # bounded by the hub command timeout; the daemon start is not abandoned halfway
            except OlcfError as exc:
                log.warning("could not start Spark worker on %s: %s", nodes[nid].name, exc.message)
                workers[nid] = {"cores": desired[nid][0], "memory_mb": desired[nid][1], "state": "failed", "detail": exc.message, "master_url": cl["master_url"]}
        cl = self._save(cl["id"], spark_workers=workers)
        # ---- wait for registration
        deadline = time.monotonic() + wait
        want = sum(1 for w in workers.values() if w.get("state") in ("running", "starting"))
        st = None
        while time.monotonic() < deadline:
            self._check(cancelled, "waiting for the Spark workers to register")
            st = self.master_status(cl)
            if st and st["alive_workers"] >= max(1, want):
                break
            self._closed.wait(0.5 if st else 1.0)
        if not st or st["alive_workers"] < 1:
            cl = self._set_state(cl, "degraded" if st else "down", "no Spark worker registered with the master in time")
            raise ProviderError(f"Spark cluster for runtime '{runtime_id}' in pool '{pool}' started but no worker registered with {cl['master_url']} within {wait:.0f}s",
                                remediation="check the Spark worker logs on the nodes and the network path between nodes")
        cl = self._save(cl["id"], last_status=st, last_status_at=now_iso())
        for w in cl["spark_workers"].values():
            if w.get("state") == "starting":
                w["state"] = "running"
        cl = self._save(cl["id"], spark_workers=cl["spark_workers"])
        return self._set_state(cl, "up" if st["alive_workers"] >= want else "degraded", "" if st["alive_workers"] >= want else f"{st['alive_workers']}/{want} workers alive")

    @staticmethod
    def _free_port(base: int, used: set[int], span: int | None = None, *, role: str = "Spark") -> int:
        """First port of ``base .. base + span - 1`` (unbounded when ``span`` is None) that is not in ``used``."""
        p = base
        while p in used:
            p += 1
        if span is not None and p > base + span - 1:
            raise ProviderError(f"all {span} {role} ports starting at {base} are in use on this node", remediation=f"widen the range (compute.{role.lower().replace(' ', '_')} settings) and the NetworkPolicy with it")
        return p

    def _worker_ports(self, cl: dict[str, Any], node: NodeInfo) -> tuple[int, int]:
        """RPC and web-UI port for the Spark worker daemon of ``cl`` on ``node``, from the configured ranges (the NetworkPolicy opens exactly those). The worker keeps the ports it
        already has, so an unchanged daemon is not restarted."""
        c = self.env.ctx.settings.compute
        used_rpc, used_ui = set(), set()
        for other in self.clusters():
            w = (other.get("spark_workers") or {}).get(node.id)
            if other["id"] != cl["id"] and w and w.get("state") in ("starting", "running"):
                used_rpc.add(int(w.get("port") or 0))
                used_ui.add(int(w.get("ui_port") or 0))
        mine = (cl.get("spark_workers") or {}).get(node.id) or {}
        ports = []
        for key, rng, used, role in (("port", c.worker_port_range, used_rpc, "Spark worker RPC"), ("ui_port", c.worker_ui_port_range, used_ui, "Spark worker UI")):
            keep = int(mine.get(key) or 0)
            ports.append(keep if rng[0] <= keep <= rng[1] and keep not in used else self._free_port(rng[0], used, rng[1] - rng[0] + 1, role=role))
        return ports[0], ports[1]

    def _start_master(self, cl: dict[str, Any], runtime_id: str, pool: str, candidates: list[NodeInfo], cancelled: Cancelled | None = None) -> dict[str, Any]:
        if not candidates:
            raise ProviderError(f"no connected node to host the Spark master for runtime '{runtime_id}' in pool '{pool}'")
        settings = self.env.ctx.settings
        # prefer the previous master node (workers may still point at it), otherwise the node with the most memory
        prev = cl.get("master_worker_id")
        node = next((n for n in candidates if n.id == prev), None) or max(candidates, key=lambda n: (n.memory_mb, n.cores, n.id))
        used_rpc, used_ui = set(), set()
        for other in self.clusters():
            if other["id"] != cl["id"] and other.get("master_worker_id") == node.id and other["state"] != "stopped":
                used_rpc.add(other.get("master_port") or 0)
                used_ui.add(other.get("ui_port") or 0)
        host = node.advertise_host(settings)
        cs = settings.compute
        port = self._free_port(cs.spark_master_port_base, used_rpc, cs.spark_master_port_span, role="Spark master RPC")
        ui_port = self._free_port(cs.spark_master_ui_port_base, used_ui, cs.spark_master_ui_port_span, role="Spark master UI")
        last: OlcfError | None = None
        for _attempt in range(3):
            conf = {"spark.ui.proxyBase": f"/v1/spark-ui/{cl['id']}", "spark.ui.reverseProxy": "true", "spark.master.rest.enabled": "false"}
            try:
                self.env.hub.request(node.id, "start_spark_master", {"runtime_id": runtime_id, "host": host, "port": port, "ui_port": ui_port, "conf": conf}, timeout=60.0)
                break
            except WorkerCommandFailed as exc:  # most likely the port is taken by a foreign process: try the next one
                last = exc
                used_rpc.add(port)
                used_ui.add(ui_port)
                port = self._free_port(cs.spark_master_port_base, used_rpc, cs.spark_master_port_span, role="Spark master RPC")
                ui_port = self._free_port(cs.spark_master_ui_port_base, used_ui, cs.spark_master_ui_port_span, role="Spark master UI")
        else:
            raise ProviderError(f"could not start a Spark master on '{node.name}': {last.message if last else 'unknown error'}") from last
        cl = self._save(cl["id"], master_worker_id=node.id, master_host=host, master_port=port, ui_port=ui_port, master_url=f"spark://{host}:{port}", ui_url=f"http://{host}:{ui_port}",
                        detail="master started")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and self.master_status(cl) is None:
            self._check(cancelled, "waiting for the Spark master")
            self._closed.wait(0.5)
        return cl

    def _start_worker(self, cl: dict[str, Any], node: NodeInfo, cores: int, mem: int, restart: bool) -> dict[str, Any]:
        if restart:
            try:
                self.env.hub.request(node.id, "stop_spark_worker", {"runtime_id": cl["runtime_id"]}, timeout=30.0)
            except OlcfError as exc:
                log.warning("stop_spark_worker on %s failed before restart: %s", node.name, exc.message)
        port, ui_port = self._worker_ports(cl, node)
        payload = {"runtime_id": cl["runtime_id"], "master_url": cl["master_url"], "cores": cores, "memory_mb": mem, "labels": {"pool": node.pool, **node.labels},
                   "work_dir": None, "port": port, "ui_port": ui_port, "conf": {"spark.ui.proxyBase": f"/v1/spark-ui/{cl['id']}"}}
        data = self.env.hub.request(node.id, "start_spark_worker", payload, timeout=60.0) or {}
        return {"cores": cores, "memory_mb": mem, "master_url": cl["master_url"], "state": "starting", "port": (data.get("port") if isinstance(data, dict) else None) or port,
                "ui_port": ui_port, "host": node.advertise_host(self.env.ctx.settings), "started_at": now_iso()}

    def release_node(self, node_id: str) -> int:
        """Stop the Spark worker daemons on a drained node (a node that hosts the master keeps it: the master is a control process, not capacity).

        Returns the number of daemons stopped. Refused (0) while any application still has executors there is the caller's duty to check.
        """
        stopped = 0
        for cl in self.clusters():
            w = (cl.get("spark_workers") or {}).get(node_id)
            if not w or w.get("state") not in ("running", "starting"):
                continue
            try:
                self.env.hub.request(node_id, "stop_spark_worker", {"runtime_id": cl["runtime_id"]}, timeout=30.0)
            except OlcfError as exc:
                log.warning("stop_spark_worker on %s: %s", node_id, exc.message)
                continue
            sw = dict(cl["spark_workers"])
            sw[node_id] = {**w, "state": "stopped"}
            self._save(cl["id"], spark_workers=sw)
            stopped += 1
        return stopped

    def stop_cluster(self, cluster_id: str) -> dict[str, Any]:
        cl = self.get_cluster(cluster_id)
        if cl is None:
            raise OlcfError("cluster not found")
        if self._busy(cluster_id):
            from olcf.core.errors import Conflict

            raise Conflict("cluster has running jobs or sessions; cancel them or drain first")
        for wid, w in (cl.get("spark_workers") or {}).items():
            if w.get("state") in ("running", "starting"):
                try:
                    self.env.hub.request(wid, "stop_spark_worker", {"runtime_id": cl["runtime_id"]}, timeout=30.0)
                except OlcfError as exc:
                    log.warning("stop_spark_worker %s: %s", wid, exc.message)
                w["state"] = "stopped"
        if cl.get("master_worker_id"):
            try:
                self.env.hub.request(cl["master_worker_id"], "stop_spark_master", {"runtime_id": cl["runtime_id"]}, timeout=30.0)
            except OlcfError as exc:
                log.warning("stop_spark_master: %s", exc.message)
        cl = self._save(cl["id"], spark_workers=cl["spark_workers"], last_status={}, master_url=None, ui_url=None, master_worker_id=None)
        return self._set_state(cl, "stopped", "stopped by an administrator")

    # ================================================================== provider interface
    def discover(self) -> Discovery:
        nodes = self.env.registry.node_infos()
        online = [n for n in nodes if self._effective_connected(n)]
        runtimes: dict[str, int] = {}
        for n in online:
            for rid in n.ready_runtimes:
                runtimes[rid] = runtimes.get(rid, 0) + 1
        return Discovery(
            self.name, self.type, ok=bool(online), detail=f"{len(online)} of {len(nodes)} nodes online", capabilities=list(self.capabilities),
            workers=[{"id": n.id, "name": n.name, "pool": n.pool, "state": n.state, "scheduling": n.scheduling} for n in nodes],
            runtimes=[{"id": r, "ready_nodes": c} for r, c in sorted(runtimes.items())],
            clusters=[{"id": c["id"], "runtime_id": c["runtime_id"], "pool": c["pool"], "state": c["state"], "master_url": c["master_url"]} for c in self.clusters()],
        )

    def prepare(self, req: PrepareRequest) -> dict[str, Any]:
        g = req.placement
        runtime_id = req.job["runtime_id"]
        pool = g.get("pool") or (req.node.pool if req.node else "default")
        cl = self.ensure_cluster(runtime_id, pool, cancelled=self._job_cancelled(req.job.get("id")))
        node: NodeInfo = req.node
        driver_host = node.advertise_host(self.env.ctx.settings)
        conf = base_spark_conf(req)
        conf.update({
            "spark.cores.max": str(max(1, g.get("executor_cores_total") or g.get("executor_cores") or 1)),
            "spark.executor.cores": str(g.get("executor_cores") or 1),
            "spark.executor.memory": spark_memory(g.get("executor_memory_mb") or 1024),
            "spark.driver.host": driver_host, "spark.driver.bindAddress": "0.0.0.0",  # noqa: S104 - the driver must accept executor connections
            "spark.driver.port": str(g["driver_port"]), "spark.blockManager.port": str(g["block_manager_port"]),
            "spark.ui.port": str(self.env.ctx.settings.compute.driver_ui_port_range[0]),  # the driver UI stays inside the range the NetworkPolicy opens
        })
        conf.update(dynamic_allocation_conf((req.job.get("resources_request") or {}), g))
        return {"master": cl["master_url"], "app_name": req.app_name, "driver_host": driver_host, "driver_port": g["driver_port"],
                "block_manager_port": g["block_manager_port"], "conf": conf, "jars": [], "packages": [], "cluster_id": cl["id"]}

    # ================================================================== spark application table
    def _sync_apps(self, cl: dict[str, Any], st: dict[str, Any]) -> None:
        now = now_iso()
        host_to_node = {w.get("host"): nid for nid, w in (cl.get("spark_workers") or {}).items() if w.get("host")}
        seen_active = set()
        with self.env.ctx.db.begin() as conn:
            for a in st.get("applications", []) + st.get("completed_applications", []):
                if not a.get("id"):
                    continue
                active = a in st.get("applications", [])
                if active:
                    seen_active.add(a["id"])
                ref = a.get("ref")
                job = fetch_one(conn, select(C.jobs.c.id, C.jobs.c.workspace_id, C.jobs.c.driver_worker_id).where(C.jobs.c.id == ref)) if ref and ref.startswith("job_") else None
                ses = fetch_one(conn, select(C.sessions.c.id, C.sessions.c.workspace_id, C.sessions.c.worker_id).where(C.sessions.c.id == ref)) if ref and ref.startswith("ses_") else None
                t = C.spark_apps.c
                cur = fetch_one(conn, select(C.spark_apps).where(t.application_id == a["id"], t.cluster_id == cl["id"]))
                vals = {"name": a.get("name"), "state": _MASTER_APP_STATES.get(str(a.get("state")).upper(), "UNKNOWN"), "cores": a.get("cores"),
                        "memory_per_executor_mb": a.get("memory_per_executor_mb"), "last_seen_at": now, "updated_at": now}
                if cur is None:
                    insert_row(conn, C.spark_apps, {"id": new_id("sap"), "application_id": a["id"], "cluster_id": cl["id"], "workspace_id": (job or ses or {}).get("workspace_id"),
                                                    "job_id": job["id"] if job else None, "session_id": ses["id"] if ses else None, "executors": [],
                                                    "driver_worker_id": (job or {}).get("driver_worker_id") or (ses or {}).get("worker_id"), "ui_url": None,
                                                    "started_at": now, "finished_at": None if active else now, "created_at": now, **vals})
                else:
                    if not active and cur["finished_at"] is None:
                        vals["finished_at"] = now
                    conn.execute(update(C.spark_apps).where(t.id == cur["id"]).values(**vals))
                if job:
                    conn.execute(update(C.jobs).where(C.jobs.c.id == job["id"], C.jobs.c.application_id.is_(None)).values(application_id=a["id"]))
            # applications that vanished from the active list are finished
            t = C.spark_apps.c
            for r in fetch_all(conn, select(t.id, t.application_id).where(t.cluster_id == cl["id"], t.state.in_(("RUNNING", "WAITING")))):
                if r["application_id"] not in seen_active:
                    conn.execute(update(C.spark_apps).where(t.id == r["id"]).values(state="FINISHED", finished_at=now, updated_at=now))
        self._sync_executors(cl, st, host_to_node)

    def _sync_executors(self, cl: dict[str, Any], st: dict[str, Any], host_to_node: dict[str, str]) -> None:
        """Best effort: executors per application from each Spark worker's own ``/json`` (Spark worker web UI)."""
        if not st.get("applications"):
            return
        per_app: dict[str, dict[str, int]] = {}
        for w in st.get("workers", []):
            if w.get("state") != "ALIVE" or not w.get("webui"):
                continue
            try:
                r = self._http.get(w["webui"].rstrip("/") + "/json/")
                doc = r.json() if r.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                continue
            node = host_to_node.get(w.get("host")) or w.get("host")
            for ex in doc.get("executors") or []:
                if ex.get("appid"):
                    d = per_app.setdefault(ex["appid"], {})
                    d[node] = d.get(node, 0) + int(ex.get("cores", 0))
        if not per_app:
            return
        with self.env.ctx.db.begin() as conn:
            for app_id, by_node in per_app.items():
                conn.execute(update(C.spark_apps).where(C.spark_apps.c.application_id == app_id, C.spark_apps.c.cluster_id == cl["id"]).values(
                    executors=[{"worker": n, "cores": c} for n, c in sorted(by_node.items())], updated_at=now_iso()))

    # ================================================================== agent hooks
    def on_hello(self, worker: dict[str, Any], hello: Any) -> None:
        """Adopt the daemons an agent reports, and mark the ones it no longer runs."""
        wid, pool = worker["id"], worker["pool"]
        daemons = list(hello.spark_daemons)
        for cl in self.clusters(pool=pool):
            sw = dict(cl.get("spark_workers") or {})
            changed = False
            wd = next((d for d in daemons if d.role == "worker" and d.runtime_id == cl["runtime_id"]), None)
            if wd is not None:
                cur = sw.get(wid) or {}
                new = {**cur, "state": "running", "master_url": wd.master_url or cur.get("master_url") or cl.get("master_url"), "cores": wd.cores or cur.get("cores", 0),
                       "memory_mb": wd.memory_mb or cur.get("memory_mb", 0), "port": wd.port or cur.get("port")}
                if new != cur:
                    sw[wid], changed = new, True
            elif wid in sw and sw[wid].get("state") in ("running", "starting"):
                sw[wid] = {**sw[wid], "state": "stopped"}
                changed = True
            if changed:
                cl = self._save(cl["id"], spark_workers=sw)
            if cl.get("master_worker_id") == wid:
                has_master = any(d.role == "master" and d.runtime_id == cl["runtime_id"] for d in daemons)
                if not has_master and cl["state"] != "stopped":
                    self._set_state(cl, "down", "master daemon is not running after the agent reconnected")
        # daemons for which the controller has no record (control-plane database restored from an older backup): adopt them
        for d in daemons:
            if d.role == "master" and not self.clusters(runtime_id=d.runtime_id, pool=pool):
                cl = self._create_cluster(d.runtime_id, pool)
                m = re.match(r"spark://([^:/]+):(\d+)", d.url or "")
                if m:
                    ui_port = d.ui_port or self.env.ctx.settings.compute.spark_master_ui_port_base
                    self._save(cl["id"], master_worker_id=wid, master_host=m.group(1), master_port=int(m.group(2)), ui_port=ui_port, master_url=d.url, ui_url=f"http://{m.group(1)}:{ui_port}",
                               state="degraded", detail="adopted from agent report")
                    log.info("adopted running Spark master %s from worker %s", d.url, worker["name"])

    def on_daemon_state(self, worker_id: str, msg: Any) -> None:
        try:
            pool = self.env.registry.get_worker(worker_id)["pool"]
        except OlcfError:
            return
        for cl in self.clusters(runtime_id=msg.runtime_id, pool=pool):
            if msg.role == "worker":
                sw = dict(cl.get("spark_workers") or {})
                if worker_id in sw:
                    state = {"running": "running", "ready": "running", "starting": "starting"}.get(msg.state, "stopped" if msg.state in ("stopped", "exited") else "failed")
                    sw[worker_id] = {**sw[worker_id], "state": state, "detail": msg.detail or ""}
                    self._save(cl["id"], spark_workers=sw)
            elif cl.get("master_worker_id") == worker_id and msg.state in ("stopped", "exited", "failed", "died"):
                self._set_state(cl, "down", msg.detail or f"master daemon {msg.state}")

    def maintain(self) -> None:
        for cl in self.clusters():
            try:
                self.refresh_cluster(cl)
            except Exception:  # noqa: BLE001
                log.exception("refresh of cluster %s failed", cl["id"])

    def close(self) -> None:
        self._closed.set()  # first: launch threads blocked in a wait loop notice at once
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._http.close()

    def reconcile(self) -> list[AppStatus]:
        out = super().reconcile()
        for cl in self.clusters():
            st = self.master_status(cl) if cl.get("ui_url") else None
            if not st:
                continue
            for a in st["applications"]:
                if a.get("ref"):
                    out.append(AppStatus({"RUNNING": "running", "WAITING": "pending"}.get(a["state"], "unknown"), a["id"], f"on cluster {cl['id']}", job_id=a["ref"] if a["ref"].startswith("job_") else None,
                                         native={"cluster_id": cl["id"], "cores": a.get("cores")}))
        return out

    def health(self, deep: bool = False) -> list[dict[str, Any]]:
        steps = []
        for cl in self.clusters():
            st = self.master_status(cl) if cl.get("ui_url") else None
            steps.append({"name": f"spark-master:{cl['runtime_id']}:{cl['pool']}", "ok": st is not None and cl["state"] in ("up", "degraded"),
                          "detail": cl["detail"] or (f"{st['alive_workers']} workers alive" if st else cl["state"]), "cluster_id": cl["id"], "state": cl["state"]})
        return steps
