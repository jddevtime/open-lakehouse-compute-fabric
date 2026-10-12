# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure scheduler/admission algorithm tests, including property-style randomised cases (no database, no network)."""

from __future__ import annotations

import random

import pytest

from olcf.compute.planner import (
    ClusterView,
    JobView,
    NodeView,
    PlannerConfig,
    PoolView,
    QueueView,
    RunningView,
    allocate_port,
    effective_priority,
    infeasible_reason,
    plan,
    standalone_worker_sizing,
)

RT = "olcf-rt-2.0.0"


def node(i, cores=8, mem=16384, pool="default", labels=None, **kw):
    return NodeView(id=f"wrk_{i}", name=f"n{i}", pool=pool, labels=labels or {}, cores_total=cores, memory_mb_total=mem,
                    runtimes_ready=frozenset({RT}), **kw)


def job(i, ws="w1", queued=990.0, prio=50, **kw):
    kw.setdefault("runtime_id", RT)
    kw.setdefault("provider", "local")
    return JobView(id=f"job_{i:05d}", workspace_id=ws, queued_at=queued + i * 0.001, priority=prio, **kw)


def run(jobs, nodes, **kw):
    kw.setdefault("now", 1000.0)
    return plan(jobs, nodes, **kw)


# --------------------------------------------------------------------------- node arithmetic
def test_usable_resources_reserve_headroom():
    n = node(1, cores=4, mem=10000)
    assert n.usable_cores == 3 and n.usable_memory_mb == 8500
    assert node(2, cores=1, mem=2000).usable_cores == 1  # a single-core machine is still usable
    assert node(3, cores=0).usable_cores == 0


def test_live_memory_pressure_reduces_free_memory():
    n = node(1, cores=8, mem=16000, live_available_memory_mb=2000)
    assert n.free_memory_mb == 2000 - 256


def test_standalone_sizing_splits_between_driver_and_executors():
    n = node(1, cores=8, mem=16384)
    cores, mem = standalone_worker_sizing(n, 1)
    assert cores == 6 and mem > 0
    assert cores + 1 <= n.usable_cores  # a driver core always remains
    c2, m2 = standalone_worker_sizing(n, 2)
    assert c2 == 3 and m2 == mem // 2
    assert standalone_worker_sizing(node(2, cores=2, mem=8000), 1) == (0, 0)  # usable=1 -> too small


def test_allocate_port():
    assert allocate_port((10, 12), {10, 11}) == 12
    assert allocate_port((10, 11), {10, 11}) is None


# --------------------------------------------------------------------------- ordering
def test_fifo_within_equal_priority():
    nodes = [node(1, cores=2)]  # usable 1 core -> one 1-core job at a time
    jobs = [job(i) for i in range(10)]
    res = run(jobs, nodes)
    assert [p.job_id for p in res.placements] == ["job_00000"]
    assert all(b.code == "resources" for b in res.blocked)


def test_priority_beats_age_but_aging_eventually_wins():
    nodes = [node(1, cores=2)]
    old_low = job(1, prio=10, queued=0.0)
    new_high = job(2, prio=50, queued=990.0)
    assert run([old_low, new_high], nodes, now=1000.0).placements[0].job_id == new_high.id
    # after waiting long enough the low priority job gets an aging bonus and wins
    cfg = PlannerConfig(aging_interval_seconds=10, aging_max_bonus=100)
    assert effective_priority(old_low, 1000.0, cfg) >= 109
    assert run([old_low, new_high], nodes, now=1000.0, cfg=cfg).placements[0].job_id == old_low.id


def test_fair_share_alternates_between_workspaces():
    nodes = [node(1, cores=9, mem=64000)]  # usable 8 cores
    jobs = [job(i, ws="a") for i in range(8)] + [job(100 + i, ws="b") for i in range(8)]
    res = run(jobs, nodes)
    by_ws = {"a": 0, "b": 0}
    ws_of = {j.id: j.workspace_id for j in jobs}
    for p in res.placements:
        by_ws[ws_of[p.job_id]] += 1
    assert by_ws == {"a": 4, "b": 4}


def test_fair_share_respects_weights_and_existing_usage():
    nodes = [node(1, cores=9, mem=64000)]
    q = {"default": QueueView("default", fair_share_weights={"a": 3.0, "b": 1.0})}
    jobs = [job(i, ws="a") for i in range(8)] + [job(100 + i, ws="b") for i in range(8)]
    res = run(jobs, nodes, queues=q)
    ws_of = {j.id: j.workspace_id for j in jobs}
    got = [ws_of[p.job_id] for p in res.placements]
    assert got.count("a") == 6 and got.count("b") == 2
    # existing running usage pushes the busy workspace back
    res2 = run(jobs, [node(1, cores=9, mem=64000, cores_allocated=0)], running=[RunningView("a", cores=4)])
    got2 = [ws_of[p.job_id] for p in res2.placements]
    assert got2.count("b") > got2.count("a")


def test_starvation_protection_closes_backfill():
    nodes = [node(1, cores=5, mem=64000)]  # usable 4
    big = job(1, queued=0.0, driver_cores=4)
    big_blocked_by = RunningView("w9", cores=1)  # not modelled in node alloc; use allocation instead
    nodes = [node(1, cores=5, mem=64000, cores_allocated=1)]
    smalls = [job(10 + i, queued=900.0, driver_cores=1) for i in range(3)]
    cfg = PlannerConfig(starvation_seconds=300, aging_interval_seconds=10_000)
    res = run([big, *smalls], nodes, now=1000.0, cfg=cfg, running=[big_blocked_by])
    # big can't fit (3 free < 4) and is starving: lower ranked smalls must be held, not backfilled
    assert res.placements == []
    codes = {b.job_id: b.code for b in res.blocked}
    assert codes[big.id] == "resources" and all(codes[s.id] == "held" for s in smalls)
    # a non-starving big job does not hold the queue: smalls backfill
    big2 = job(1, queued=900.0, driver_cores=4)
    res2 = run([big2, *smalls], nodes, now=1000.0, cfg=cfg)
    assert len(res2.placements) == 3


def test_retry_backoff_is_respected():
    j = job(1, not_before=2000.0)
    res = run([j], [node(1)], now=1000.0)
    assert res.placements == [] and res.blocked[0].code == "backoff"
    assert len(run([j], [node(1)], now=2001.0).placements) == 1


# --------------------------------------------------------------------------- constraints
def test_cordoned_draining_offline_nodes_are_never_used():
    nodes = [node(1, scheduling="cordoned"), node(2, scheduling="draining"), node(3, state="offline"), node(4, state="pending")]
    res = run([job(1)], nodes)
    assert res.placements == [] and res.blocked[0].code == "no_node"
    res = run([job(1)], [*nodes, node(5, state="degraded")])
    assert res.placements[0].node_id == "wrk_5"


def test_runtime_must_be_ready_on_node():
    n = NodeView(id="wrk_1", name="n1", cores_total=8, memory_mb_total=16000, runtimes_ready=frozenset({"other"}))
    res = run([job(1)], [n])
    assert res.blocked[0].code == "no_runtime"
    assert run([job(1, runtime_id="other")], [n]).placements


def test_pool_labels_and_node_constraints():
    nodes = [node(1, pool="gpu", labels={"gpu": "a100", "site": "dc1"}), node(2, pool="default", labels={"site": "dc2"})]
    assert run([job(1, pool="gpu")], nodes).placements[0].node_id == "wrk_1"
    assert run([job(1, labels={"site": "dc2"})], nodes).placements[0].node_id == "wrk_2"
    assert run([job(1, labels={"gpu": "*"})], nodes).placements[0].node_id == "wrk_1"
    assert run([job(1, node="n2")], nodes).placements[0].node_id == "wrk_2"
    assert run([job(1, node="wrk_2", pool="gpu")], nodes).blocked[0].code == "constraints"
    assert run([job(1, labels={"site": "dc9"})], nodes).blocked[0].code == "constraints"


def test_reserved_pool_only_admits_listed_workspaces():
    nodes = [node(1, pool="secure")]
    pools = {"secure": PoolView("secure", reserved=True, workspace_ids=frozenset({"w-ok"}))}
    assert run([job(1, ws="w-no")], nodes, pools=pools).blocked[0].code == "constraints"
    assert run([job(1, ws="w-ok")], nodes, pools=pools).placements


def test_queue_allowed_pools():
    nodes = [node(1, pool="a"), node(2, pool="b")]
    q = {"default": QueueView("default", allowed_pools=("b",))}
    assert run([job(1)], nodes, queues=q).placements[0].node_id == "wrk_2"


# --------------------------------------------------------------------------- quotas
def test_queue_concurrency_and_pause():
    nodes = [node(1, cores=33, mem=200000)]
    q = {"default": QueueView("default", max_concurrency=3)}
    res = run([job(i) for i in range(10)], nodes, queues=q)
    assert len(res.placements) == 3
    res = run([job(i) for i in range(10)], nodes, queues=q, running=[RunningView("w1")])
    assert len(res.placements) == 2
    paused = {"default": QueueView("default", paused=True)}
    res = run([job(1)], nodes, queues=paused)
    assert res.placements == [] and res.blocked[0].code == "paused"


def test_workspace_limits_queue_and_workspace_level():
    nodes = [node(1, cores=33, mem=200000)]
    q = {"default": QueueView("default", workspace_limits={"w1": {"max_concurrency": 2}})}
    jobs = [job(i, ws="w1") for i in range(5)] + [job(50 + i, ws="w2") for i in range(5)]
    res = run(jobs, nodes, queues=q)
    got = {"w1": 0, "w2": 0}
    for p in res.placements:
        got["w1" if p.job_id < "job_00050" else "w2"] += 1
    assert got == {"w1": 2, "w2": 5}
    res = run(jobs, nodes, workspace_quotas={"w2": {"max_cores": 3}})
    assert sum(1 for p in res.placements if p.job_id >= "job_00050") == 3


def test_queue_core_limit_counts_executors_for_standalone():
    nodes = [node(i, cores=9, mem=64000) for i in (1, 2)]
    q = {"default": QueueView("default", max_cores=6)}
    j = job(1, provider="standalone", driver_cores=1, executor_cores=1, min_executors=1, max_cores=10)
    res = run([j], nodes, queues=q)
    p = res.placements[0]
    assert p.driver_cores + p.executor_cores_total <= 6 and p.executor_cores_total == 5


# --------------------------------------------------------------------------- admission
def test_admission_never_oversubscribes_cores_or_memory():
    nodes = [node(1, cores=5, mem=8000)]  # usable 4 cores, 6800 MB
    jobs = [job(i, driver_cores=1, driver_memory_mb=2048) for i in range(10)]
    res = run(jobs, nodes)
    assert len(res.placements) == 3  # memory is the binding constraint: 3*2048 = 6144 <= 6800 < 4*2048
    assert sum(p.driver_memory_mb for p in res.placements) <= 6800


def test_running_allocations_are_subtracted():
    n = node(1, cores=5, mem=64000, cores_allocated=3)
    assert len(run([job(i) for i in range(3)], [n]).placements) == 1


def test_infeasible_requests_are_detected_up_front():
    nodes = [node(1, cores=4, mem=4000), node(2, cores=2, mem=2000)]
    assert infeasible_reason(job(1, driver_cores=64), nodes) and "usable cores" in infeasible_reason(job(1, driver_cores=64), nodes)
    assert infeasible_reason(job(1, driver_memory_mb=999_999), nodes)
    assert infeasible_reason(job(1), nodes) is None
    assert infeasible_reason(job(1, pool="nope"), nodes)
    assert infeasible_reason(job(1, driver_cores=64), []) is None  # no fleet yet: don't reject


def test_driver_ports_are_unique_per_node():
    nodes = [node(1, cores=33, mem=200000)]
    jobs = [job(i, engine="spark", provider="local") for i in range(5)]
    res = run(jobs, nodes)
    ports = [p.driver_port for p in res.placements] + [p.block_manager_port for p in res.placements]
    assert len(ports) == len(set(ports)) == 10 and all(ports)


def test_python_engine_never_gets_cluster_resources():
    res = run([job(1, engine="python", provider="standalone")], [node(1)])
    assert res.placements[0].provider == "local" and res.placements[0].executor_cores_total == 0


# --------------------------------------------------------------------------- spark standalone
def test_auto_provider_prefers_standalone_when_a_node_can_host_it_else_local_with_a_reason():
    j = job(1, provider=None)
    one = run([j], [node(1)]).placements[0]
    assert one.provider == "standalone" and one.note is None                      # a single 8-core machine hosts master + worker + driver
    two = run([j], [node(1), node(2)]).placements[0]
    assert two.provider == "standalone"
    small = run([j], [node(1, cores=2, mem=4096)])
    assert small.blocked == [] and small.placements[0].provider == "local"
    assert "local mode" in small.placements[0].note and "usable core" in small.placements[0].note


@pytest.mark.parametrize(("cores", "mem", "expected"), [(2, 4096, "local"), (2, 8192, "local"), (4, 8192, "standalone"), (4, 16384, "standalone"),
                                                       (8, 16384, "standalone"), (16, 32768, "standalone"), (1, 2048, "local")])
@pytest.mark.parametrize("flexible", [True, False])
def test_single_node_inventories_always_place_a_default_spark_job(cores, mem, expected, flexible):
    """Simulated machines of 1-16 cores: a job with default requests is never blocked or rejected, only placed smaller / in local mode."""
    n = node(1, cores=cores, mem=mem)
    j = job(1, provider=None, flexible=flexible)
    assert infeasible_reason(j, [n]) is None
    res = run([j], [n])
    assert res.blocked == [] and len(res.placements) == 1
    p = res.placements[0]
    assert p.provider == expected
    assert p.driver_cores + p.executor_cores_total <= max(n.usable_cores, 1)
    assert p.driver_memory_mb + p.executor_memory_mb * p.executors <= max(n.usable_memory_mb, 1024)
    if expected == "local":
        assert p.executor_cores_total == 0 and p.note and "local mode" in p.note and "needs at least" in p.note      # the reason is visible
    if flexible and expected == "standalone" and cores >= 8:
        assert p.executor_cores_total >= 3                                         # default requests scale up with the machine


def test_flexible_defaults_scale_with_the_machine_but_explicit_requests_are_kept():
    small, big = run([job(1, provider=None, flexible=True)], [node(1, cores=4, mem=8192)]).placements[0], \
        run([job(1, provider=None, flexible=True)], [node(1, cores=16, mem=32768)]).placements[0]
    assert big.executor_cores_total > small.executor_cores_total
    explicit = run([job(1, provider="standalone", flexible=False, executor_cores=2, min_executors=1, max_executors=2)], [node(1, cores=16, mem=32768)]).placements[0]
    assert explicit.executor_cores == 2 and explicit.executors == 2 and explicit.note is None


def test_explicit_local_request_is_not_second_guessed():
    p = run([job(1, provider="local")], [node(1, cores=16, mem=32768)]).placements[0]
    assert p.provider == "local" and p.note is None


def test_standalone_grants_executors_from_cluster_capacity_and_marks_virtual_usage():
    nodes = [node(1, cores=8, mem=16384), node(2, cores=8, mem=16384)]  # worker = 6 cores each = 12
    jobs = [job(i, provider="standalone", executor_cores=2, executor_memory_mb=1024, min_executors=1, max_executors=4) for i in range(3)]
    res = run(jobs, nodes)
    got = [p.executor_cores_total for p in res.placements]
    assert got == [8, 4]  # third job waits: the (virtual) cluster has no executor cores left
    assert all(p.cluster_key == (RT, "default") for p in res.placements)
    assert [b.code for b in res.blocked] == ["resources"]


def test_standalone_waits_when_cluster_busy_and_never_below_min_executors():
    nodes = [node(1, cores=8), node(2, cores=8)]
    cl = ClusterView(RT, "default", "standalone", "clu_1", {"wrk_1": 6, "wrk_2": 6}, {"wrk_1": 8000, "wrk_2": 8000}, cores_used=11, memory_used_mb=0)
    j = job(1, provider="standalone", executor_cores=2, min_executors=2)
    res = run([j], nodes, clusters=[cl])
    assert res.placements == [] and "free executor cores" in res.blocked[0].reason
    cl2 = ClusterView(RT, "default", "standalone", "clu_1", {"wrk_1": 6, "wrk_2": 6}, {"wrk_1": 8000, "wrk_2": 8000}, cores_used=8)
    assert run([j], nodes, clusters=[cl2]).placements[0].executor_cores_total == 4


def test_standalone_rejects_executor_larger_than_any_worker():
    nodes = [node(1, cores=8, mem=8000), node(2, cores=8, mem=8000)]
    res = run([job(1, provider="standalone", executor_memory_mb=60000)], nodes)
    assert res.blocked[0].code == "resources" and "largest Spark worker" in res.blocked[0].reason


def test_remote_provider_needs_no_node():
    cfg = PlannerConfig(remote_providers=frozenset({"kubernetes"}))
    res = run([job(1, provider="kubernetes")], [], cfg=cfg)
    assert res.placements and res.placements[0].node_id is None


# --------------------------------------------------------------------------- properties
def _random_world(rng: random.Random):
    nodes = [node(i, cores=rng.choice([2, 3, 5, 9, 17]), mem=rng.choice([4096, 8192, 32768]), pool=rng.choice(["default", "gpu"]),
                  labels=rng.choice([{}, {"site": "a"}, {"site": "b"}]), cores_allocated=rng.choice([0, 0, 1]),
                  state=rng.choice(["online"] * 5 + ["offline"]), scheduling=rng.choice(["active"] * 6 + ["cordoned"])) for i in range(rng.randint(0, 6))]
    jobs = []
    for i in range(rng.randint(0, 40)):
        jobs.append(job(i, ws=rng.choice(["a", "b", "c"]), prio=rng.choice([10, 50, 90]), queued=rng.uniform(0, 900),
                        provider=rng.choice(["local", "standalone", None]), driver_cores=rng.choice([1, 1, 2, 4]),
                        driver_memory_mb=rng.choice([512, 1024, 2048]), executor_cores=rng.choice([1, 2]),
                        min_executors=rng.choice([0, 1, 2]), max_executors=rng.choice([None, 2, 5]),
                        pool=rng.choice([None, None, "default", "gpu"]), labels=rng.choice([{}, {}, {"site": "a"}]),
                        engine=rng.choice(["spark", "spark", "python"])))
    q = {"default": QueueView("default", max_concurrency=rng.choice([None, 3, 10]), max_cores=rng.choice([None, 8, 40]))}
    return nodes, jobs, q


@pytest.mark.parametrize("seed", range(60))
def test_property_plan_is_safe_and_deterministic(seed):
    rng = random.Random(seed)
    nodes, jobs, queues = _random_world(rng)
    res = plan(jobs, nodes, queues=queues, now=1000.0)
    by_job = {j.id: j for j in jobs}
    node_by = {n.id: n for n in nodes}
    cores, mem, ports = {}, {}, {}
    for p in res.placements:
        j = by_job[p.job_id]
        if p.node_id is None:
            continue
        n = node_by[p.node_id]
        # constraints
        assert n.schedulable and RT in n.runtimes_ready
        assert j.pool in (None, n.pool) and all(n.labels.get(k) == v for k, v in j.labels.items())
        cores[n.id] = cores.get(n.id, 0) + p.driver_cores
        mem[n.id] = mem.get(n.id, 0) + p.driver_memory_mb
        for port in (p.driver_port, p.block_manager_port):
            if port:
                assert (n.id, port) not in ports
                ports[(n.id, port)] = 1
        if p.cluster_key:
            assert p.executor_cores_total >= j.min_executor_cores and p.executor_cores_total % j.executor_cores == 0
            assert p.executor_cores_total <= j.wanted_executor_cores
        if j.engine == "python":
            assert p.executor_cores_total == 0
    for nid, c in cores.items():  # never oversubscribe usable resources
        assert c <= node_by[nid].free_cores and mem[nid] <= node_by[nid].free_memory_mb
    qv = queues["default"]
    if qv.max_concurrency is not None:
        assert len(res.placements) <= qv.max_concurrency
    if qv.max_cores is not None:
        assert sum(p.driver_cores + p.executor_cores_total for p in res.placements) <= qv.max_cores
    # every job is either placed or blocked, exactly once
    ids = [p.job_id for p in res.placements] + [b.job_id for b in res.blocked]
    assert sorted(ids) == sorted(by_job)
    # deterministic regardless of input order
    shuffled = jobs[:]
    rng.shuffle(shuffled)
    nshuf = nodes[:]
    rng.shuffle(nshuf)
    res2 = plan(shuffled, nshuf, queues=queues, now=1000.0)
    assert [(p.job_id, p.node_id, p.executor_cores_total) for p in res.placements] == [(p.job_id, p.node_id, p.executor_cores_total) for p in res2.placements]


@pytest.mark.parametrize("seed", range(20))
def test_property_identical_jobs_in_a_workspace_run_fifo(seed):
    """If an identical-shape job queued earlier is blocked while a later one is placed, FIFO was violated."""
    rng = random.Random(seed)
    nodes = [node(i, cores=rng.choice([3, 5, 9]), mem=16384) for i in range(rng.randint(1, 3))]
    jobs = [job(i, ws="a", queued=float(i)) for i in range(rng.randint(5, 30))]
    res = plan(jobs, nodes, now=1000.0)
    placed = {p.job_id for p in res.placements}
    ordered = [j.id for j in jobs]
    flags = [i in placed for i in ordered]
    assert flags == sorted(flags, reverse=True)  # all True then all False


def test_plan_handles_500_job_queue_quickly_and_in_order():
    import time

    nodes = [node(1, cores=65, mem=262144)]
    jobs = [job(i, queued=float(i)) for i in range(500)]
    t0 = time.perf_counter()
    res = plan(jobs, nodes, now=1000.0)
    assert time.perf_counter() - t0 < 2.0
    assert [p.job_id for p in res.placements] == [f"job_{i:05d}" for i in range(64)]
