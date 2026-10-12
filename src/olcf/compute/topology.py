# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Topology, connectivity tests, Spark smoke test and health checks for the control panel (CONTRACTS 4.6, section 8).

* :meth:`TopologyService.topology` assembles ``GET /v1/compute/topology``: summary, control-plane replicas and components, Spark clusters (with the *proxy* UI path, never the
  internal URL), nodes (resources, live metrics, 120-sample history, components, runtimes, running jobs) and queues. Every field is always present.
* :meth:`test_worker` executes a probe suite *from the node's perspective* through the agent (``probe``): DNS/TCP/HTTP(S) to the control plane, TCP and master JSON of every Spark
  cluster, reachability of every registered storage endpoint, disk, clock skew and runtime verification. :meth:`test_all` runs it for all online nodes concurrently and returns a
  matrix. :meth:`spark_smoke` submits a real tiny distributed PySpark job and reports which nodes hosted executors.
* Health checks (quick + deep) are registered with the foundation registry: ``scheduler``, ``worker-fleet``, ``runtimes`` and ``spark-master:<runtime>`` for every known runtime.
* Replica heartbeats (kv ``compute.replica.<node>``) make the control-plane node list identical on every API replica.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from olcf.api.deps import Authed, Ctx
from olcf.compute import schema as C
from olcf.compute.registry import (
    ACTIVE_JOB_STATES,
    ACTIVE_SESSION_STATES,
    ComputeRegistry,
    NodeInfo,
    summarize_metrics,
    worker_allocation_summary,
)
from olcf.core.context import AppContext
from olcf.core.errors import Conflict, NotFound, OlcfError
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.hostruntime import HOST_FAMILY, HOST_RUNTIME_ID
from olcf.core.models import (
    JobKind,
    JobSource,
    JobSubmission,
    Origin,
    OriginKind,
    PolicySpec,
    ResourceRequest,
    RuntimeRequest,
)
from olcf.core.timeutil import age_seconds, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one
from olcf.security.rbac import SYSTEM_PRINCIPAL, Principal
from olcf.version import __version__

log = logging.getLogger("olcf.compute.topology")

REPLICA_PREFIX = "compute.replica."
REPLICA_FRESH_SECONDS = 30.0
TOPOLOGY_CATEGORIES = ("control-plane", "compute", "agents", "security")
SMOKE_WORKSPACE = "wks_system"
SMOKE_URL = "/v1/compute/spark-smoke"
SMOKE_TERMINAL = ("succeeded", "failed", "cancelled", "timed_out")
SMOKE_SWEEP_SECONDS = 2.0
SMOKE_JOB_GRACE_SECONDS = 60  # the job's own timeout is only a backstop: the smoke deadline (``smoke_tick``) fires first and says why

#: The distributed smoke job. It is submitted through the normal job path, so the runtime bootstrap (``spark`` below) is built from the job spec: its
#: ``spark.master`` is the standalone master chosen by the scheduler (``local[*]`` only when the scheduler had no cluster). It never builds its own
#: ``SparkSession``. It waits (bounded) for the requested number of executors, runs a repartitioned RDD whose tasks sleep so they spread over every
#: executor, and reports for each executor who ran what: host name, the agent's node id (``OLCF_WORKER_ID``, exported by the Spark Worker daemon to its
#: executors - the host name alone cannot tell two agents on one machine apart), executor id and work directory.
SMOKE_SCRIPT = '''
import json, os, socket, time

sc = spark.sparkContext
parts = int(os.environ.get("SMOKE_PARTITIONS", "8"))
want = int(os.environ.get("SMOKE_EXECUTORS", "2"))
distributed_master = not str(sc.master).startswith("local")

def executor_count():
    # SparkStatusTracker lists the driver as well as the executors
    return max(0, len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1)

deadline = time.time() + float(os.environ.get("SMOKE_EXECUTOR_WAIT_SECONDS", "45"))
if distributed_master:
    # dummy job to make Spark request the executors, then wait until the requested number registered (or the deadline)
    sc.parallelize(range(want * 2), want * 2).count()
    while executor_count() < want and time.time() < deadline:
        time.sleep(0.5)
registered = executor_count()

total_cores = max(1, int(sc.defaultParallelism))  # registered executor cores on a cluster, local cores otherwise
parts = max(parts, 2 * total_cores)  # at least two waves of tasks per core: every executor gets work even when one starts a little later

def where(index, rows):
    # runs INSIDE a task, on an executor: who am I?
    time.sleep(1.0)
    cwd = os.getcwd()
    yield {"host": socket.gethostname(), "node": os.environ.get("OLCF_WORKER_ID"), "pid": os.getpid(), "cwd": cwd,
           "worker_dir": os.path.dirname(os.path.dirname(cwd)), "executor": os.path.basename(cwd), "partition": index}

rows = sc.parallelize(range(parts), parts).mapPartitionsWithIndex(where).collect()
by_exec = {}
for r in rows:
    key = (r["node"], r["worker_dir"], r["executor"], r["host"])
    e = by_exec.setdefault(key, {"node": r["node"], "host": r["host"], "executor": r["executor"], "worker_dir": r["worker_dir"], "tasks": 0})
    e["tasks"] += 1
executors = sorted(by_exec.values(), key=lambda e: (str(e["node"]), str(e["executor"])))
hosts = sorted({r["host"] for r in rows})
total = sc.parallelize(range(1000), 4).sum()
print("@@OLCF@@ " + json.dumps({"event": "executor_hosts", "hosts": [{"host": h, "tasks": sum(1 for r in rows if r["host"] == h)} for h in hosts],
                                "executors": executors, "executors_registered": registered}))
print("@@OLCF@@ " + json.dumps({"event": "smoke_result", "sum": total, "tasks": len(rows), "master": sc.master, "applicationId": sc.applicationId,
                                "master_is_cluster": distributed_master, "executors_requested": want, "partitions": parts, "total_cores": total_cores}))
assert total == 499500, "unexpected sum"
spark.stop()
'''


class NoSparkRuntime(Conflict):
    """No online worker has a ready Spark runtime (HTTP 409, code ``no_spark_runtime``): the smoke test is refused instead of queueing a job that cannot start."""
    code = "no_spark_runtime"


class SmokeAlreadyRunning(Conflict):
    """A Spark smoke test is already in progress (HTTP 409, code ``smoke_already_running``; ``details.id`` / ``details.poll_url`` name it)."""
    code = "smoke_already_running"


class SmokeRequest(BaseModel):
    """Request body of the Spark smoke test: runtime, partitions, executors, timeout and pool."""
    runtime_id: str | None = None
    partitions: int = Field(8, ge=2, le=64)
    executors: int = Field(2, ge=1, le=16)
    timeout_seconds: int = Field(240, ge=10, le=1800)
    pool: str | None = None


def _step(name: str, ok: bool, detail: str, ms: float = 0.0, **extra: Any) -> dict[str, Any]:
    return {"name": name, "ok": bool(ok), "detail": detail, "ms": round(float(ms), 1), **extra}


class TopologyService:
    """Registered on the compute service as ``topology_service``."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: Any, jobs: Any, providers: Any, runtimes: Any, sessions: Any) -> None:
        self.ctx, self.registry, self.hub, self.jobs, self.providers, self.runtimes, self.sessions = ctx, registry, hub, jobs, providers, runtimes, sessions
        self.scheduler: Any = None
        self._smoke_swept = 0.0
        self._registered_checks: set[str] = set()
        self._started = time.time()

    # ================================================================== replicas
    def heartbeat(self) -> None:
        """Publish this replica's liveness (any replica may call; the leader flag is what ``ctx.leader`` says right now)."""
        val = {"node_id": self.ctx.node_id, "leader": self.ctx.leader.is_leader(), "version": __version__, "started_at": self._started, "ts": time.time(),
               "workers_connected": len(self.hub.connected_ids())}
        key = REPLICA_PREFIX + self.ctx.node_id
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == key).values(value=val, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=key, value=val, updated_at=now_iso()))

    def replicas(self) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(S.kv).where(S.kv.c.key.like(REPLICA_PREFIX + "%")))
        out = []
        for r in rows:
            v = dict(r["value"] or {})
            if v.get("node_id") == self.ctx.node_id:  # this replica knows better than its last published heartbeat
                v.update(ts=time.time(), leader=self.ctx.leader.is_leader(), workers_connected=len(self.hub.connected_ids()))
            age = time.time() - float(v.get("ts") or 0)
            if age > REPLICA_FRESH_SECONDS * 4:
                with self.ctx.db.begin() as conn:  # long gone: forget it
                    conn.execute(S.kv.delete().where(S.kv.c.key == r["key"]))
                continue
            out.append({"node_id": v.get("node_id"), "leader": bool(v.get("leader")) and age <= REPLICA_FRESH_SECONDS, "version": v.get("version"),
                        "uptime_seconds": round(time.time() - float(v.get("started_at") or time.time()), 1), "heartbeat_age_seconds": round(age, 1),
                        "alive": age <= REPLICA_FRESH_SECONDS, "workers_connected": v.get("workers_connected", 0)})
        if not any(o["node_id"] == self.ctx.node_id for o in out):
            out.append({"node_id": self.ctx.node_id, "leader": self.ctx.leader.is_leader(), "version": __version__, "uptime_seconds": round(time.time() - self._started, 1),
                        "heartbeat_age_seconds": 0.0, "alive": True, "workers_connected": len(self.hub.connected_ids())})
        return sorted(out, key=lambda o: str(o["node_id"]))

    # ================================================================== topology
    def topology(self) -> dict[str, Any]:
        db = self.ctx.db
        with db.connect() as conn:
            workers = fetch_all(conn, select(C.workers).order_by(C.workers.c.name))
            clusters = fetch_all(conn, select(C.clusters).order_by(C.clusters.c.created_at))
            active_jobs = fetch_all(conn, select(C.jobs.c.id, C.jobs.c.name, C.jobs.c.state, C.jobs.c.driver_worker_id, C.jobs.c.resources_granted, C.jobs.c.cluster_id, C.jobs.c.workspace_id)
                                    .where(C.jobs.c.state.in_(ACTIVE_JOB_STATES)))
            sessions = fetch_all(conn, select(C.sessions.c.id, C.sessions.c.worker_id, C.sessions.c.resources, C.sessions.c.state, C.sessions.c.kind, C.sessions.c.hidden)
                                 .where(C.sessions.c.state.in_(ACTIVE_SESSION_STATES)))
            apps = fetch_all(conn, select(C.spark_apps).where(C.spark_apps.c.state.in_(("RUNNING", "WAITING"))))
        alloc, _ports = worker_allocation_summary(active_jobs, sessions, [c for c in clusters if c["state"] in ("up", "degraded", "starting")])
        by_cluster: dict[str, list[dict[str, Any]]] = {}
        for a in apps:
            by_cluster.setdefault(a["cluster_id"], []).append(a)
        nodes = [self._node(w, alloc.get(w["id"], (0, 0)), active_jobs, sessions, clusters) for w in workers]
        cl_out = [self._cluster(c, by_cluster.get(c["id"], [])) for c in clusters]
        live = [n for n in nodes if n["state"] in ("online", "degraded")]
        executors = sum(1 for a in apps for e in (a["executors"] or []) if isinstance(e, dict))
        summary = {"nodes_total": sum(1 for n in nodes if n["state"] != "revoked"), "nodes_online": len(live), "cores_total": sum(n["resources"]["cores"] for n in live),
                   "cores_used": sum(n["resources"]["cores_allocated"] for n in live), "memory_total_mb": sum(n["resources"]["memory_mb"] for n in live),
                   "memory_used_mb": sum(n["resources"]["memory_allocated_mb"] for n in live), "jobs_running": len(active_jobs), "jobs_queued": self.jobs.counts()["queued"],
                   "executors": executors, "sessions_active": len([s for s in sessions if not s["hidden"]])}
        stats = self.jobs.queue_stats()
        queues = []
        for q in self.registry.list_queues():
            s = stats.get(q["name"]) or {}
            oldest = s.get("oldest_queued_at")
            queues.append({"name": q["name"], "depth": s.get("depth", 0), "running": s.get("running", 0), "paused": bool(q.get("paused")),
                           "oldest_wait_seconds": round(age_seconds(oldest) or 0.0, 1) if oldest else 0})
        return {"generated_at": now_iso(), "summary": summary, "control_plane": {"nodes": self.replicas(), "components": self._components()}, "clusters": cl_out, "nodes": nodes, "queues": queues}

    def _components(self) -> list[dict[str, Any]]:
        out = []
        cats = {d["name"]: d["category"] for d in self.ctx.health.describe()}
        for name in self.ctx.health.names():
            if cats.get(name) not in TOPOLOGY_CATEGORIES:
                continue
            try:
                out.append(self.ctx.health.run(name).to_dict())
            except Exception as exc:  # noqa: BLE001 - one broken check never breaks the topology
                out.append(ComponentHealth(name, Status.UNKNOWN, f"check failed: {exc}", category=cats.get(name, "platform")).to_dict())
        for c in out:
            c.pop("labels", None)
            c.pop("steps", None)
            c.pop("mode", None)
        return out

    def _node(self, w: dict[str, Any], alloc: tuple[int, int], jobs: list[dict[str, Any]], sessions: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> dict[str, Any]:
        n = NodeInfo.from_row(w)
        hist = w.get("history") or {}
        m = summarize_metrics(w.get("metrics"))
        age = age_seconds(w.get("last_seen_at"))
        comps = [dict(c) for c in (w.get("components") or []) if isinstance(c, dict)]
        have = {c.get("name") for c in comps}
        if "agent" not in have:
            comps.append({"name": "agent", "status": "ok" if w["state"] in ("online", "degraded") else "down", "detail": f"agent {w.get('agent_version') or '?'} ({'connected' if w.get('connected') else 'not connected'})"})
        if "control-plane-link" not in have:
            late = age is not None and age > 2 * self.ctx.settings.compute.worker_heartbeat_seconds
            comps.append({"name": "control-plane-link", "status": "ok" if w.get("connected") and not late else ("degraded" if w.get("connected") else "down"),
                          "detail": f"last heartbeat {age:.1f}s ago" if age is not None else "never seen"})
        for c in clusters:
            sw = (c.get("spark_workers") or {}).get(w["id"])
            if sw and f"spark-worker:{c['runtime_id']}" not in have:
                comps.append({"name": f"spark-worker:{c['runtime_id']}", "status": {"running": "ok", "starting": "degraded"}.get(sw.get("state"), "down"), "detail": f"{sw.get('state')} ({sw.get('cores')} cores, {sw.get('memory_mb')} MB)"})
        runtimes = [{"id": r["id"], "state": r.get("state"), "verified": bool(r.get("verified")), "fingerprint": r.get("fingerprint"),
                     **({"family": r.get("family"), "python": r.get("python"), "no_spark": True} if r.get("no_spark") else {})}  # the built-in host-Python runtime says what it is
                    for r in (w.get("runtimes") or []) if isinstance(r, dict) and "id" in r]
        running = [{"job_id": j["id"], "name": j["name"], "state": j["state"], "workspace_id": j["workspace_id"]} for j in jobs if j["driver_worker_id"] == w["id"]]
        kernels = [{"session_id": s["id"], "state": s["state"], "kind": s["kind"]} for s in sessions if s["worker_id"] == w["id"] and not s["hidden"]]
        return {"id": w["id"], "name": w["name"], "hostname": w.get("hostname"), "address": w.get("address"), "state": w["state"], "scheduling": w["scheduling"], "pool": w["pool"],
                "labels": w.get("labels") or {}, "embedded": bool(w.get("embedded")), "agent_version": w.get("agent_version"), "os": w.get("os"), "arch": w.get("arch"),
                "connected": bool(w.get("connected")), "heartbeat_age_seconds": round(age, 1) if age is not None else None,
                "resources": {"cores": n.cores, "memory_mb": n.memory_mb, "disk_gb": n.disk_gb, "cores_allocated": alloc[0], "memory_allocated_mb": alloc[1]},
                "metrics": {k: m.get(k) for k in ("cpu_percent", "mem_percent", "disk_percent", "load1", "net_rx_bps", "net_tx_bps")},
                "history": {"cpu": list(hist.get("cpu") or []), "mem": list(hist.get("mem") or []), "ts": list(hist.get("ts") or [])},
                "components": comps, "runtimes": runtimes, "running_jobs": running, "sessions": kernels, "notes": w.get("notes") or {},
                "cert_expires_at": w.get("cert_expires_at"), "token_expires_at": w.get("token_expires_at")}

    def _cluster(self, c: dict[str, Any], apps: list[dict[str, Any]]) -> dict[str, Any]:
        st = c.get("last_status") or {}
        master_state = {"ALIVE": "ok", "STANDBY": "degraded", "RECOVERING": "degraded"}.get(str(st.get("state")), "ok" if st else "down")
        return {"id": c["id"], "runtime_id": c["runtime_id"], "family": c.get("family"), "provider": c["provider"], "pool": c["pool"], "state": c["state"], "detail": c.get("detail") or "",
                "master": {"url": c.get("master_url"), "ui_url": f"/v1/spark-ui/{c['id']}/" if c.get("ui_url") else None, "state": master_state, "alive_workers": st.get("alive_workers", 0),
                           "cores_total": st.get("cores_total", 0), "cores_used": st.get("cores_used", 0), "memory_total_mb": st.get("memory_total_mb", 0),
                           "memory_used_mb": st.get("memory_used_mb", 0),
                           "applications": [{"id": a["application_id"], "ref": a["id"], "name": a.get("name"), "cores": a.get("cores"), "state": a["state"], "job_id": a.get("job_id"),
                                             "session_id": a.get("session_id"), "executors": [{"worker": e.get("worker"), "cores": e.get("cores")} for e in (a["executors"] or []) if isinstance(e, dict)]}
                                            for a in apps]},
                "workers": sorted((c.get("spark_workers") or {}).keys()), "last_status_at": c.get("last_status_at")}

    # ================================================================== node tests
    def probe_checks(self, worker: dict[str, Any]) -> list[dict[str, Any]]:
        """The probe suite for one node (every check is executed *on the node*)."""
        checks: list[dict[str, Any]] = []
        api = urlsplit(self.jobs.api_url())
        host = api.hostname or "localhost"
        port = api.port or (443 if api.scheme == "https" else 80)
        checks.append({"kind": "dns", "name": "dns:control-plane", "host": host})
        checks.append({"kind": "tcp", "name": "tcp:control-plane", "host": host, "port": port})
        checks.append({"kind": "http", "name": "http:control-plane", "url": f"{self.jobs.api_url()}/v1/health", "use_agent_ca": api.scheme == "https"})
        with self.ctx.db.connect() as conn:
            clusters = fetch_all(conn, select(C.clusters).where(C.clusters.c.state != "stopped"))
            conns = fetch_all(conn, select(S.connections.c.name, S.connections.c.type, S.connections.c.config).where(S.connections.c.deleted_at.is_(None)).limit(200))
        for cl in clusters:
            if cl.get("master_url"):
                checks.append({"kind": "spark_master", "name": f"spark-master:{cl['runtime_id']}", "master_url": cl["master_url"]})
            if cl.get("ui_url"):
                checks.append({"kind": "spark_master", "name": f"spark-master-ui:{cl['runtime_id']}", "url": cl["ui_url"].rstrip("/") + "/json/"})
        seen: set[str] = set()
        for c in conns:
            cfg = c["config"] or {}
            for key in ("root_uri", "uri", "endpoint", "endpoint_url", "base_url", "url", "account_url"):
                v = cfg.get(key)
                if isinstance(v, str) and "://" in v and v not in seen and not v.startswith(("jdbc:", "file:")):
                    seen.add(v)
                    checks.append({"kind": "storage", "name": f"storage:{c['name']}", "uri": v})
                    break
        checks.append({"kind": "disk", "name": "disk", "min_free_gb": 1.0})
        checks.append({"kind": "clock", "name": "clock", "max_skew_seconds": 5.0, "server_time": now_iso()})
        for r in worker.get("runtimes") or []:
            if isinstance(r, dict) and r.get("state") == "ready":
                checks.append({"kind": "runtime", "name": f"runtime:{r['id']}", "runtime_id": r["id"]})
        return checks

    def test_worker(self, ref: str) -> dict[str, Any]:
        w = self.registry.find_worker(ref)
        steps: list[dict[str, Any]] = []
        t0 = time.monotonic()
        if w["state"] == "revoked":
            return {"ok": False, "worker_id": w["id"], "name": w["name"], "steps": [_step("agent-link", False, "the worker is revoked")], "duration_ms": 0.0, "checked_at": now_iso()}
        try:
            t1 = time.monotonic()
            self.hub.request(w["id"], "ping", {}, timeout=10.0)
            steps.append(_step("agent-link", True, "agent answered ping through the control channel", (time.monotonic() - t1) * 1000))
        except OlcfError as exc:
            steps.append(_step("agent-link", False, exc.message, (time.monotonic() - t0) * 1000, remediation="start the agent / check its network path to the control plane"))
            return self._finish_test(w, steps, t0)
        try:
            res = self.hub.request(w["id"], "probe", {"checks": self.probe_checks(w)}, timeout=90.0) or {}
            for c in res.get("checks", []):
                extra = {"kind": c.get("kind")}
                steps.append(_step(str(c.get("name") or c.get("kind")), bool(c.get("ok")), str(c.get("detail") or ""), float(c.get("ms") or 0.0), **extra))
        except OlcfError as exc:
            steps.append(_step("probe", False, exc.message, (time.monotonic() - t0) * 1000))
        return self._finish_test(w, steps, t0)

    def _finish_test(self, w: dict[str, Any], steps: list[dict[str, Any]], t0: float) -> dict[str, Any]:
        ok = all(s["ok"] for s in steps)
        result = {"ok": ok, "worker_id": w["id"], "name": w["name"], "steps": steps, "duration_ms": round((time.monotonic() - t0) * 1000, 1), "checked_at": now_iso()}
        notes = dict(w.get("notes") or {})
        notes["last_test"] = {"ok": ok, "at": result["checked_at"], "failed": [s["name"] for s in steps if not s["ok"]][:10]}
        try:
            self.registry.update_worker(w["id"], notes=notes)
        except OlcfError:
            pass
        return result

    def test_all(self) -> dict[str, Any]:
        workers = [w for w in self.registry.list_workers(include_revoked=False)]
        t0 = time.monotonic()
        results: dict[str, dict[str, Any]] = {}
        if workers:
            with ThreadPoolExecutor(max_workers=min(16, len(workers)), thread_name_prefix="olcf-nodetest") as pool:
                futs = {w["name"]: pool.submit(self.test_worker, w["id"]) for w in workers}
                for name, f in futs.items():
                    try:
                        results[name] = f.result(timeout=150)
                    except Exception as exc:  # noqa: BLE001
                        results[name] = {"ok": False, "name": name, "steps": [_step("test", False, f"{type(exc).__name__}: {exc}")]}
        columns: list[str] = []
        for r in results.values():
            for s in r["steps"]:
                if s["name"] not in columns:
                    columns.append(s["name"])
        rows = [{"node": name, "ok": r["ok"], "results": {s["name"]: {"ok": s["ok"], "ms": s["ms"], "detail": s["detail"]} for s in r["steps"]}} for name, r in sorted(results.items())]
        return {"ok": bool(results) and all(r["ok"] for r in results.values()), "generated_at": now_iso(), "duration_ms": round((time.monotonic() - t0) * 1000, 1), "nodes": results,
                "matrix": {"checks": columns, "rows": rows}}

    # ================================================================== spark smoke test
    # The smoke test is asynchronous: ``start_spark_smoke`` submits the job and returns at once, ``spark_smoke_status`` builds the report from the job (so it can be
    # polled by id, also after the request that started it is long gone), ``smoke_tick`` (leader hook) enforces the deadline, ``cancel_spark_smoke`` stops it.
    def _smoke_candidates(self, req: SmokeRequest) -> dict[str, Any]:
        """Online workers that could host a Spark job: their ready Spark runtimes (the built-in host-Python runtime is not one)."""
        online = [w for w in self.registry.list_workers(pool=req.pool, include_revoked=False)
                  if w.get("connected") and w["state"] in ("online", "degraded") and w.get("scheduling", "active") == "active"]
        ready: dict[str, list[str]] = {}
        for w in online:
            for r in (w.get("runtimes") or []):
                if not isinstance(r, dict) or r.get("state") != "ready" or r.get("no_spark") or r.get("family") == HOST_FAMILY or r.get("id") == HOST_RUNTIME_ID:
                    continue
                if req.runtime_id and r.get("id") != req.runtime_id:
                    continue
                ready.setdefault(str(r["id"]), []).append(w["name"])
        return {"online": online, "ready": ready}

    def _smoke_preflight(self, req: SmokeRequest) -> None:
        """Fail fast (409 ``no_spark_runtime``) instead of queueing a job that can never start."""
        provider = str(self.registry.setting("compute.default_provider", "auto") or "auto")
        if provider not in ("auto", "", "standalone", "local"):
            return  # a remote provider (Kubernetes, Livy, ...) runs Spark itself: the workers' runtimes do not matter
        found = self._smoke_candidates(req)
        online, ready = found["online"], found["ready"]
        if ready:
            return
        scope = f" in pool '{req.pool}'" if req.pool else ""
        if not online:
            message = f"no worker is online{scope}: the Spark smoke test needs at least one connected, schedulable worker with a Spark runtime"
        elif req.runtime_id:
            message = f"runtime '{req.runtime_id}' is not ready on any of the {len(online)} online worker(s){scope}"
        else:
            message = f"no Spark runtime is ready on any of the {len(online)} online worker(s){scope} (the built-in host-Python runtime cannot run Spark)"
        raise NoSparkRuntime(message, details={"online_workers": len(online), "spark_ready_workers": 0, "spark_runtimes": [], "requested_runtime": req.runtime_id, "pool": req.pool},
                        remediation="enrol a worker with 'olcf worker enroll-token' (and uncordon it), then install a runtime on it with 'olcf runtime install <id> --worker <name>'; "
                                    "'olcf runtime list' shows what is installed")

    def _running_smoke(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.name == "olcf-spark-smoke", C.jobs.c.workspace_id == SMOKE_WORKSPACE,
                                                        C.jobs.c.state.not_in(SMOKE_TERMINAL)).order_by(C.jobs.c.created_at.desc()))
        return next((r for r in rows if (r.get("origin") or {}).get("diagnostic") == "spark-smoke"), None)

    def _smoke_row(self, job_id: str) -> dict[str, Any]:
        try:
            row = self.jobs.get_row(job_id)
        except NotFound:
            raise NotFound(f"spark smoke test {job_id} not found") from None
        if row["name"] != "olcf-spark-smoke" or (row.get("origin") or {}).get("diagnostic") != "spark-smoke":
            raise NotFound(f"spark smoke test {job_id} not found")
        return row

    def start_spark_smoke(self, principal: Principal, req: SmokeRequest) -> dict[str, Any]:
        """Submit the distributed PySpark job (system workspace) and return its status at once (the caller polls ``spark_smoke_status``)."""
        principal.require("compute.manage")
        running = self._running_smoke()
        if running:
            raise SmokeAlreadyRunning(f"a Spark smoke test is already running ({running['id']})", details={"id": running["id"], "poll_url": f"{SMOKE_URL}/{running['id']}"},
                            remediation=f"follow it with 'olcf compute smoke --status {running['id']}' or stop it with POST {SMOKE_URL}/{running['id']}/cancel")
        self._smoke_preflight(req)
        placement: dict[str, Any] = {"pool": req.pool} if req.pool else {}
        sub = JobSubmission(
            kind=JobKind.SPARK_JOB, workspace_id=SMOKE_WORKSPACE, name="olcf-spark-smoke", source=JobSource(format="py", content=SMOKE_SCRIPT, filename="smoke.py"),
            runtime=RuntimeRequest(runtime_id=req.runtime_id) if req.runtime_id else RuntimeRequest(),
            resources=ResourceRequest(driver_cores=1, driver_memory_mb=512, executor_cores=1, executor_memory_mb=512, min_executors=min(req.executors, 2), max_executors=req.executors),
            policy=PolicySpec(timeout_seconds=req.timeout_seconds + SMOKE_JOB_GRACE_SECONDS, priority=90, placement=placement), env={"SMOKE_PARTITIONS": str(req.partitions), "SMOKE_EXECUTORS": str(req.executors)},
            origin=Origin(kind=OriginKind.STANDALONE, details={"diagnostic": "spark-smoke", "requested_by": principal.id, "timeout_seconds": req.timeout_seconds,
                                                                "deadline_at": iso_in(req.timeout_seconds)}))
        rec = self.jobs.submit(SYSTEM_PRINCIPAL, sub, internal=True)
        self.ctx.audit.record(principal, "compute.spark_smoke", resource_type="job", resource_id=rec.id,
                              details={"runtime": rec.runtime_id, "timeout_seconds": req.timeout_seconds, "pool": req.pool})
        return self.spark_smoke_status(rec.id)

    def cancel_spark_smoke(self, principal: Principal | None, job_id: str, *, reason: str = "spark smoke test cancelled") -> dict[str, Any]:
        if principal is not None:
            principal.require("compute.manage")
        self._smoke_row(job_id)
        self.jobs.cancel(None, job_id, reason=reason)
        if principal is not None:
            self.ctx.audit.record(principal, "compute.spark_smoke_cancel", resource_type="job", resource_id=job_id, details={"reason": reason})
        return self.spark_smoke_status(job_id)

    def smoke_tick(self) -> None:
        """Leader hook: stop smoke tests past their deadline, queued or running, so none is left behind."""
        now = time.monotonic()
        if now - self._smoke_swept < SMOKE_SWEEP_SECONDS:
            return
        self._smoke_swept = now
        try:
            with self.ctx.db.connect() as conn:
                rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.name == "olcf-spark-smoke", C.jobs.c.workspace_id == SMOKE_WORKSPACE, C.jobs.c.state.not_in(SMOKE_TERMINAL)))
        except Exception:  # noqa: BLE001
            log.exception("spark smoke sweep failed")
            return
        for row in rows:
            details = row.get("origin") or {}
            if details.get("diagnostic") != "spark-smoke":
                continue
            limit = int(details.get("timeout_seconds") or 0)
            elapsed = age_seconds(row["created_at"])
            if not limit or elapsed is None or elapsed < limit:
                continue
            if row["state"] == "cancel_requested":
                continue  # already being stopped; the job layer escalates
            reason = f"spark smoke test timed out after {limit} s"
            try:
                self.jobs.cancel(None, row["id"], reason=reason)
                self.ctx.audit.record(SYSTEM_PRINCIPAL, "compute.spark_smoke_timeout", resource_type="job", resource_id=row["id"],
                                      details={"timeout_seconds": limit, "state": row["state"], "wait_reason": row.get("wait_reason")})
            except Exception:  # noqa: BLE001
                log.exception("could not stop the timed-out smoke test %s", row["id"])

    def spark_smoke_status(self, job_id: str) -> dict[str, Any]:
        """The report of a smoke test: progress while it runs, the node/executor evidence once it has finished."""
        row = self._smoke_row(job_id)
        state = row["state"]
        finished = state in SMOKE_TERMINAL
        details = row.get("origin") or {}
        limit = int(details.get("timeout_seconds") or 0)
        events = self.jobs.timeline(job_id)
        timed_out = state == "timed_out" or any("spark smoke test timed out" in str(e.get("reason") or "") for e in events)
        phase = "done" if finished else {"cancel_requested": "cancelling", "running": "running", "provisioning": "starting"}.get(state, "queued")
        wait_reason = row.get("wait_reason") if state in ("queued", "accepted", "validating", "provisioning") else None
        elapsed = age_seconds(row["created_at"]) or 0.0
        if finished and row.get("finished_at"):
            from olcf.core.timeutil import from_iso

            elapsed = max(0.0, (from_iso(row["finished_at"]) - from_iso(row["created_at"])).total_seconds())
        base: dict[str, Any] = {"id": job_id, "job_id": job_id, "poll_url": f"{SMOKE_URL}/{job_id}", "finished": finished, "ok": (state == "succeeded") if finished else None, "phase": phase,
                                "state": state, "timed_out": timed_out, "timeout_seconds": limit or None, "deadline_at": details.get("deadline_at"),
                                "elapsed_seconds": round(elapsed, 1), "runtime_id": row["runtime_id"], "provider": row["provider"], "wait_reason": wait_reason}
        if timed_out:
            base["message"] = f"the Spark smoke test timed out after {limit} s and was stopped" + (f" (it was still waiting: {wait_reason})" if wait_reason else "")
            if not row.get("started_at"):
                base["message"] += " (it never started: it was still waiting in the queue for a node with capacity)" if not wait_reason else ""
        elif phase == "done":
            base["message"] = {"succeeded": "the Spark smoke test passed", "cancelled": "the Spark smoke test was cancelled", "failed": "the Spark smoke test failed"}.get(state, f"the Spark smoke test ended: {state}")
        elif phase == "cancelling":
            base["message"] = "stopping the Spark smoke test"
        elif phase == "running":
            base["message"] = "the Spark application is running"
        elif phase == "starting":
            base["message"] = "starting the Spark application"
        else:
            base["message"] = f"waiting in the queue: {wait_reason}" if wait_reason else "waiting for a node to run the Spark application"
        base.update(self._smoke_evidence(row, events, finished))
        return base

    def _smoke_evidence(self, row: dict[str, Any], events: list[dict[str, Any]], finished: bool) -> dict[str, Any]:
        """Which nodes hosted executors, from the job's events and the master's application record."""
        job_id = row["id"]
        hosts: list[dict[str, Any]] = []
        reported: list[dict[str, Any]] = []
        smoke: dict[str, Any] = {}
        registered: int | None = None
        for e in events:
            if e["event"] == "executor_hosts":
                data = e["data"] or {}
                hosts = list(data.get("hosts") or [])
                reported = [x for x in (data.get("executors") or []) if isinstance(x, dict)]
                registered = data.get("executors_registered")
            if e["event"] == "smoke_result":
                smoke = dict(e["data"] or {})
        with self.ctx.db.connect() as conn:
            app = fetch_one(conn, select(C.spark_apps).where(C.spark_apps.c.job_id == job_id).order_by(C.spark_apps.c.created_at.desc()).limit(1))
            workers = fetch_all(conn, select(C.workers.c.id, C.workers.c.name, C.workers.c.hostname, C.workers.c.address))
        executors = [e for e in ((app or {}).get("executors") or []) if isinstance(e, dict)]
        nodes = self._map_hosts(hosts, executors, workers, reported)
        master = smoke.get("master")
        on_cluster = str(master or "").startswith("spark://")
        # "distributed" means: the application really ran on a standalone cluster (not local[*]) and its tasks / executors were hosted by at least two agents
        distributed = on_cluster and len(nodes) >= 2
        addresses = {str(n.get("address") or n.get("hostname") or "") for n in nodes}
        single_address = len(nodes) >= 2 and len(addresses) == 1
        ok = row["state"] == "succeeded"
        note: str | None = None
        if finished:
            if not on_cluster and ok:
                note = f"the job ran with master {master or 'local'} - no Spark standalone cluster was used, so nothing was distributed (is a runtime with Spark installed on at least one node?)"
            elif not distributed:
                note = "tasks ran on a single node (a cluster of one, or only one node has capacity); enrol a second node to verify distribution"
            elif single_address:
                note = (f"executors ran on {len(nodes)} separate agents, but all of them share the address {next(iter(addresses))} (one machine): the Spark protocol was exercised "
                        "between processes, the network path between machines was not")
        return {"master": master, "master_is_cluster": on_cluster, "partitions": smoke.get("partitions"), "total_cores": smoke.get("total_cores"),
                "application_id": row["application_id"] or smoke.get("applicationId"), "tasks": smoke.get("tasks"), "sum": smoke.get("sum"), "hosts": hosts, "executors": reported,
                "executors_registered": registered, "nodes_with_executors": nodes, "distributed": distributed, "single_address": single_address, "failure": row["failure"], "note": note}

    def spark_smoke(self, principal: Principal, req: SmokeRequest) -> dict[str, Any]:
        """Synchronous form (``olcf doctor``-style callers): start, wait (bounded by the deadline) and return the final report."""
        t0 = time.monotonic()
        doc = self.start_spark_smoke(principal, req)
        stop = t0 + float(req.timeout_seconds) + 30.0
        while not doc["finished"] and time.monotonic() < stop:
            time.sleep(0.5)
            doc = self.spark_smoke_status(doc["id"])
        if not doc["finished"]:
            doc = self.cancel_spark_smoke(None, doc["id"], reason=f"spark smoke test timed out after {req.timeout_seconds} s")
        return {**doc, "duration_seconds": round(time.monotonic() - t0, 1)}

    @staticmethod
    def _map_hosts(hosts: list[dict[str, Any]], executors: list[dict[str, Any]], workers: list[dict[str, Any]], reported: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Which agents hosted executors / tasks. Sources, most precise first: the node id each executor reports itself (``OLCF_WORKER_ID``), the executors the
        master lists per worker, and - for older runners - the host names of the tasks (ambiguous when several agents share a machine)."""
        by_id = {w["id"]: w for w in workers}
        out: dict[str, dict[str, Any]] = {}

        def entry(w: dict[str, Any]) -> dict[str, Any]:
            return out.setdefault(w["id"], {"worker_id": w["id"], "name": w["name"], "hostname": w["hostname"], "address": w.get("address"), "cores": None, "tasks": None,
                                            "executors": []})

        for e in executors:
            w = by_id.get(str(e.get("worker")))
            if w:
                entry(w)["cores"] = e.get("cores")
        for r in reported or []:
            w = by_id.get(str(r.get("node")))
            if w:
                n = entry(w)
                n["tasks"] = (n["tasks"] or 0) + int(r.get("tasks") or 0)
                n["executors"].append({"executor": r.get("executor"), "host": r.get("host"), "tasks": r.get("tasks")})
        if not reported:
            for h in hosts:
                name = str(h.get("host") or "")
                for w in workers:
                    cands = {str(w.get("hostname") or "").lower(), str(w.get("address") or "").lower(), str(w["name"]).lower()}
                    short = {c.split(".")[0] for c in cands if c}
                    if name.lower() in cands or name.lower().split(".")[0] in short:
                        entry(w)["tasks"] = h.get("tasks")
                        break
        return sorted(out.values(), key=lambda x: x["name"])

    # ================================================================== health checks
    def register_health(self) -> None:
        h = self.ctx.health
        h.register(HealthCheck("scheduler", "compute", "Job scheduler and admission control", self._scheduler_quick, self._scheduler_deep, ttl=5))
        h.register(HealthCheck("worker-fleet", "compute", "Worker nodes: online/degraded/offline", self._fleet_quick, self._fleet_deep, ttl=5))
        h.register(HealthCheck("runtimes", "compute", "Spark runtimes: installed and verified per node", self._runtimes_quick, self._runtimes_deep, ttl=10))
        self.sync_runtime_checks()

    def sync_runtime_checks(self) -> None:
        """One ``spark-master:<runtime>`` check per known runtime (called at start, after runtime registration and periodically)."""
        try:
            rows = self.runtimes.rows()
        except Exception:  # noqa: BLE001
            return
        for r in rows:
            if r["family"] == HOST_FAMILY:
                continue  # the host-Python runtime has no Spark, so no master to check
            name = f"spark-master:{r['id']}"
            if name in self._registered_checks:
                continue
            self._registered_checks.add(name)
            rid = r["id"]
            self.ctx.health.register(HealthCheck(name, "compute", f"Spark standalone master for runtime {rid}", lambda rid=rid: self._master_quick(rid),
                                                 lambda rid=rid: self._master_deep(rid), ttl=10))

    def _scheduler_quick(self) -> ComponentHealth:
        reps = self.replicas()
        leaders = [r for r in reps if r["leader"]]
        local = self.scheduler.stats if self.scheduler else {}
        if not self.scheduler or not (self.scheduler._thread and self.scheduler._thread.is_alive()):  # noqa: SLF001
            return ComponentHealth("scheduler", Status.DOWN, "the scheduler thread is not running on this replica", category="compute")
        if not leaders:
            return ComponentHealth("scheduler", Status.DEGRADED, "no replica currently holds the scheduler lease (a new leader is being elected)", category="compute")
        counts = self.jobs.counts()
        detail = f"leader {leaders[0]['node_id']}; {counts['queued']} queued, {counts['running']} running"
        if local.get("last_error"):
            return ComponentHealth("scheduler", Status.DEGRADED, f"{detail}; last error: {local['last_error']}", category="compute", labels=counts)
        return ComponentHealth("scheduler", Status.OK, detail, category="compute", labels={**counts, "leader": leaders[0]["node_id"], "ticks": local.get("ticks", 0)})

    def _scheduler_deep(self) -> ComponentHealth:
        steps = []
        ok = True
        try:
            inp = self.scheduler.build_inputs(time.time(), with_queued=False)
            steps.append({"step": "read-fleet-state", "ok": True, "detail": f"{len(inp['nodes'])} nodes, {len(inp['clusters'])} clusters"})
            from olcf.compute.planner import JobView, plan

            probe = JobView(id="probe", workspace_id="probe", driver_cores=1, driver_memory_mb=256, engine="python", queued_at=time.time())
            res = plan([probe], inp["nodes"], inp["clusters"], now=time.time(), cfg=self.scheduler.planner_config())
            placed = bool(res.placements)
            steps.append({"step": "dry-run-placement", "ok": placed, "detail": "a 1-core job would be placed" if placed else f"not placeable: {res.blocked[0].reason if res.blocked else 'no capacity'}"})
            ok = placed
        except Exception as exc:  # noqa: BLE001
            steps.append({"step": "plan", "ok": False, "detail": f"{type(exc).__name__}: {exc}"})
            ok = False
        base = self._scheduler_quick()
        status = base.status if ok else (Status.DEGRADED if base.status == Status.OK else base.status)
        return ComponentHealth("scheduler", status, base.detail, category="compute", steps=steps, mode="deep")

    def _fleet_quick(self) -> ComponentHealth:
        ws = self.registry.list_workers(include_revoked=False)
        if not ws:
            return ComponentHealth("worker-fleet", Status.DEGRADED, "no worker nodes are enrolled (jobs cannot run)", category="compute",
                                   labels={"nodes": 0}, steps=[{"step": "enrol", "ok": False, "detail": "create an enrollment token: POST /v1/workers/enrollment-tokens"}])
        c = {"online": 0, "degraded": 0, "offline": 0, "pending": 0}
        for w in ws:
            c[w["state"]] = c.get(w["state"], 0) + 1
        detail = ", ".join(f"{n} {k}" for k, n in c.items() if n)
        if c["online"] + c["degraded"] == 0:
            return ComponentHealth("worker-fleet", Status.DOWN, f"no node is reachable ({detail})", category="compute", labels=c)
        if c["offline"] or c["degraded"] or c["pending"]:
            return ComponentHealth("worker-fleet", Status.DEGRADED, detail, category="compute", labels=c)
        return ComponentHealth("worker-fleet", Status.OK, detail, category="compute", labels=c)

    def _fleet_deep(self) -> ComponentHealth:
        res = self.test_all()
        steps = [{"step": name, "ok": r["ok"], "detail": "; ".join(f"{s['name']}: {s['detail']}" for s in r["steps"] if not s["ok"])[:300] or "all checks passed"} for name, r in res["nodes"].items()]
        base = self._fleet_quick()
        if res["nodes"] and not res["ok"]:
            status = Status.DEGRADED if base.status == Status.OK else base.status
        else:
            status = base.status
        return ComponentHealth("worker-fleet", status, base.detail, category="compute", steps=steps, labels=base.labels, mode="deep")

    def _runtimes_quick(self) -> ComponentHealth:
        rows = self.runtimes.rows()
        live = [w for w in self.registry.list_workers(include_revoked=False) if w["state"] in ("online", "degraded")]
        if not rows:
            return ComponentHealth("runtimes", Status.DOWN, "no runtime is registered", category="compute")
        ready: dict[str, int] = {}
        for w in live:
            for r in w.get("runtimes") or []:
                if isinstance(r, dict) and r.get("state") == "ready":
                    ready[r["id"]] = ready.get(r["id"], 0) + 1
        if not live:
            return ComponentHealth("runtimes", Status.UNKNOWN, f"{len(rows)} runtime(s) registered; no node is online to report installs", category="compute")
        if not ready:
            return ComponentHealth("runtimes", Status.DEGRADED, "no runtime is ready on any online node", category="compute",
                                   steps=[{"step": "install", "ok": False, "detail": "POST /v1/runtimes/<id>/install"}])
        detail = "; ".join(f"{rid}: ready on {n}/{len(live)} node(s)" for rid, n in sorted(ready.items()))
        partial = any(n < len(live) for n in ready.values())
        if set(ready) == {HOST_RUNTIME_ID}:  # the first-run state: Python notebooks and jobs work, Spark is an optional download
            detail += "; no Spark runtime is installed yet (optional: Spark notebooks and jobs wait for one; `olcf runtime install`)"
        return ComponentHealth("runtimes", Status.DEGRADED if partial else Status.OK, detail, category="compute", labels={"ready": ready})

    def _runtimes_deep(self) -> ComponentHealth:
        steps = []
        for w in self.registry.list_workers(include_revoked=False):
            if w["state"] not in ("online", "degraded"):
                continue
            rts = [r["id"] for r in (w.get("runtimes") or []) if isinstance(r, dict) and r.get("state") == "ready"]
            if not rts:
                steps.append({"step": f"{w['name']}", "ok": False, "detail": "no ready runtime"})
                continue
            try:
                res = self.hub.request(w["id"], "probe", {"checks": [{"kind": "runtime", "name": f"runtime:{r}", "runtime_id": r} for r in rts]}, timeout=120.0) or {}
                for c in res.get("checks", []):
                    steps.append({"step": f"{w['name']}:{c.get('name')}", "ok": bool(c.get("ok")), "detail": str(c.get("detail") or "")[:300]})
            except OlcfError as exc:
                steps.append({"step": w["name"], "ok": False, "detail": exc.message})
        base = self._runtimes_quick()
        status = base.status if all(s["ok"] for s in steps) else (Status.DEGRADED if base.status == Status.OK else base.status)
        return ComponentHealth("runtimes", status, base.detail, category="compute", steps=steps, labels=base.labels, mode="deep")

    def _clusters_of(self, runtime_id: str) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, select(C.clusters).where(C.clusters.c.runtime_id == runtime_id))

    def _master_quick(self, runtime_id: str) -> ComponentHealth:
        name = f"spark-master:{runtime_id}"
        cls = self._clusters_of(runtime_id)
        if not cls:
            return ComponentHealth(name, Status.UNKNOWN, "no Spark standalone cluster exists for this runtime yet (clusters start on demand)", category="compute")
        worst = Status.OK
        parts = []
        for c in cls:
            s = {"up": Status.OK, "degraded": Status.DEGRADED, "starting": Status.DEGRADED, "stopped": Status.UNKNOWN, "down": Status.DOWN}.get(c["state"], Status.UNKNOWN)
            parts.append(f"{c['pool']}: {c['state']}" + (f" ({c['detail']})" if c.get("detail") else ""))
            if s.rank > worst.rank or (worst == Status.OK and s == Status.UNKNOWN):
                worst = s
        if all(c["state"] == "stopped" for c in cls):
            worst = Status.UNKNOWN
        return ComponentHealth(name, worst, "; ".join(parts), category="compute")

    def _master_deep(self, runtime_id: str) -> ComponentHealth:
        name = f"spark-master:{runtime_id}"
        steps = []
        for c in self._clusters_of(runtime_id):
            if c["state"] == "stopped":
                steps.append({"step": f"{c['pool']}: state", "ok": True, "detail": "stopped (starts on demand)"})
                continue
            try:
                prov = self.providers.get(c["provider"])
                fresh = prov.refresh_cluster(c) if hasattr(prov, "refresh_cluster") else c
                steps.append({"step": f"{c['pool']}: master", "ok": fresh["state"] in ("up", "degraded"), "detail": fresh.get("detail") or fresh["state"]})
                if fresh.get("master_worker_id"):
                    res = self.hub.request(fresh["master_worker_id"], "probe", {"checks": [{"kind": "spark_master", "name": "master-from-node", "url": (fresh.get("ui_url") or "").rstrip("/") + "/json/"}]}, timeout=20.0) or {}
                    for chk in res.get("checks", []):
                        steps.append({"step": f"{c['pool']}: reachable from its own node", "ok": bool(chk.get("ok")), "detail": str(chk.get("detail") or "")[:300]})
            except OlcfError as exc:
                steps.append({"step": f"{c['pool']}: probe", "ok": False, "detail": exc.message})
        base = self._master_quick(runtime_id)
        status = base.status if all(s["ok"] for s in steps) else (Status.DEGRADED if base.status in (Status.OK, Status.UNKNOWN) else base.status)
        return ComponentHealth(name, status, base.detail, category="compute", steps=steps, mode="deep")


# =========================================================================== routes
topology_router = APIRouter(tags=["compute"])
worker_test_router = APIRouter(tags=["workers"])


def _svc(ctx: Any) -> TopologyService:
    return ctx.service("compute").topology_service


@topology_router.get("/topology")
def get_topology(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    return _svc(ctx).topology()


@topology_router.post("/test")
def test_compute(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    res = _svc(ctx).test_all()
    ctx.audit.record(principal, "compute.test", resource_type="compute", details={"ok": res["ok"], "nodes": len(res["nodes"])})
    return res


@topology_router.post("/spark-smoke", status_code=202)
def spark_smoke(principal: Authed, ctx: Ctx, response: Response, body: SmokeRequest | None = None) -> dict[str, Any]:
    """Start the distributed Spark smoke test. Returns 202 at once with the job id and ``poll_url``; 409 ``no_spark_runtime`` when no worker could run it."""
    doc = _svc(ctx).start_spark_smoke(principal, body or SmokeRequest())
    response.headers["Location"] = doc["poll_url"]
    response.headers["Retry-After"] = "2"
    return doc


@topology_router.get("/spark-smoke/{job_id}")
def spark_smoke_status(job_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    return _svc(ctx).spark_smoke_status(job_id)


@topology_router.post("/spark-smoke/{job_id}/cancel")
def spark_smoke_cancel(job_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Stop a running or queued smoke test (idempotent)."""
    return _svc(ctx).cancel_spark_smoke(principal, job_id)


@worker_test_router.post("/{worker_id}/test")
def test_worker(worker_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    res = _svc(ctx).test_worker(worker_id)
    ctx.audit.record(principal, "worker.test", resource_type="worker", resource_id=res["worker_id"], details={"ok": res["ok"], "failed": [s["name"] for s in res["steps"] if not s["ok"]][:10]})
    return res

