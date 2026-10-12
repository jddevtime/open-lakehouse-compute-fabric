# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Launch threads never outlive their job or their provider, and in-process compute locks never leak between stacks.

* ``ComputeRegistry`` keeps its in-process locks per ``Database`` OBJECT (weak keys), not per ``id(db)``: Python reuses ids, so a later stack (test) could share a
  lock with a collected one and queue behind a stale launch thread.
* ``db_lock(cancel=...)`` ends the wait with ``LockAborted``; the Standalone provider's wait loops (pool lock, room for workers, master start, worker registration)
  end with ``LaunchAborted`` as soon as the job is no longer provisioning (cancelled, failed, finished) or the provider is closed, instead of polling a dead master for
  ``start_timeout_seconds`` while holding the pool lock.
"""

from __future__ import annotations

import gc
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select

from olcf.compute.registry import ComputeRegistry, LockAborted
from olcf.db import schema as S
from olcf.db.engine import Database
from tests.compute.fake_agent import JobPlan, wait_until

LOCK_ROW = "lock.compute.cluster.standalone.default"


def _registry(tmp_path, name: str) -> tuple[ComputeRegistry, Database]:
    db = Database(f"sqlite:///{tmp_path / (name + '.db')}")
    db.migrate()
    return ComputeRegistry(SimpleNamespace(db=db, node_id=f"node-{name}", settings=None)), db  # type: ignore[arg-type]


# ------------------------------------------------------------------------------------------------------------------------- in-process lock isolation
def test_local_locks_are_per_database_object_not_per_id(tmp_path):
    r1, db1 = _registry(tmp_path, "one")
    r2, _db2 = _registry(tmp_path, "two")
    same_db = ComputeRegistry(SimpleNamespace(db=db1, node_id="other-replica", settings=None))  # type: ignore[arg-type]
    assert r1._local_lock("k") is same_db._local_lock("k"), "registries on one database queue in memory"  # noqa: SLF001
    assert r1._local_lock("k") is not r2._local_lock("k"), "another database must never share a lock"  # noqa: SLF001


def test_a_collected_database_leaves_no_lock_behind(tmp_path):
    r, db = _registry(tmp_path, "gone")
    r._local_lock("k")  # noqa: SLF001
    assert db in ComputeRegistry._locks  # noqa: SLF001
    del r, db
    gc.collect()
    # a new database - even one that Python gives the old object's id - starts with no locks
    r2, db2 = _registry(tmp_path, "fresh")
    assert ComputeRegistry._locks.get(db2) is None  # noqa: SLF001
    r2._local_lock("k")  # noqa: SLF001
    assert len(ComputeRegistry._locks[db2]) == 1  # noqa: SLF001


def test_a_held_lock_of_one_stack_does_not_block_another(tmp_path):
    r1, _ = _registry(tmp_path, "a")
    r2, _ = _registry(tmp_path, "b")
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        with r1.db_lock("pool", ttl=60, wait=60):
            held.set()
            release.wait(30)

    t = threading.Thread(target=hold)
    t.start()
    try:
        assert held.wait(10)
        t0 = time.monotonic()
        with r2.db_lock("pool", ttl=60, wait=5):
            pass
        assert time.monotonic() - t0 < 2.0
    finally:
        release.set()
        t.join(10)


def test_db_lock_wait_ends_when_cancelled(tmp_path):
    r, _ = _registry(tmp_path, "w")
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        with r.db_lock("pool", ttl=60, wait=60):
            held.set()
            release.wait(30)

    t = threading.Thread(target=hold)
    t.start()
    try:
        assert held.wait(10)
        stop_at = time.monotonic() + 0.5
        t0 = time.monotonic()
        with pytest.raises(LockAborted, match="job cancelled"), r.db_lock("pool", ttl=60, wait=60, cancel=lambda: "job cancelled" if time.monotonic() > stop_at else None):
            pass
        assert time.monotonic() - t0 < 5.0, "the waiter must give up soon after the cancel, not after its 60 s wait"
    finally:
        release.set()
        t.join(10)


# ------------------------------------------------------------------------------------------------------------------------- the provider's wait loops
def _lock_row_exists(st: Any) -> bool:
    with st.ctx.db.connect() as conn:
        return conn.execute(select(S.kv.c.key).where(S.kv.c.key == LOCK_ROW)).first() is not None


def _never_register(agent: Any) -> None:
    """The Spark worker daemon "starts" but never registers with the master: the launch waits for registration (``start_timeout_seconds``, 90 s)."""

    async def start_worker(p: dict[str, Any]) -> dict[str, Any]:
        agent.spark_workers[p["runtime_id"]] = {"master_url": p["master_url"], "cores": p["cores"], "memory_mb": p["memory_mb"]}
        return {"pid": 5001, "port": 7078}

    agent._cmd_start_spark_worker = start_worker  # noqa: SLF001


def _spark_job(st: Any, ws: str) -> dict[str, Any]:
    return st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1})


def test_cancelling_the_job_ends_the_launch_wait_and_frees_the_pool_lock(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("abort-a", cores=8, memory_mb=16384)
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    _never_register(a)
    st.wait_worker(a)
    provider = st.compute.providers.get("standalone")
    polls: list[float] = []
    original = provider.master_status

    def counting(cl: dict[str, Any]) -> Any:
        polls.append(time.monotonic())
        return original(cl)

    provider.master_status = counting
    try:
        job = _spark_job(st, ws)
        wait_until(lambda: a.commands_of("start_spark_worker") and _lock_row_exists(st), 30, message="the launch to wait for the worker to register")
        assert st.job(job["id"])["state"] == "provisioning"
        time.sleep(1.5)
        assert len(polls) >= 2, "the launch is polling the master"
        st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)
        wait_until(lambda: st.job(job["id"])["state"] in ("cancelled", "failed"), 30, message="the job to end")
        wait_until(lambda: not _lock_row_exists(st), 10, message="the pool lock to be released by the launch thread (it was still waiting for its workers)")
        settled = len(polls)
        time.sleep(3.0)
        assert len(polls) - settled <= 1, f"the launch thread kept polling the master after the cancel ({len(polls) - settled} more polls)"
    finally:
        provider.master_status = original


def test_closing_the_provider_ends_the_launch_wait(make_stack):
    st = make_stack()
    ws = st.workspace()
    a = st.agent("abort-c", cores=8, memory_mb=16384)
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    _never_register(a)
    st.wait_worker(a)
    job = _spark_job(st, ws)
    wait_until(lambda: a.commands_of("start_spark_worker") and _lock_row_exists(st), 30, message="the launch to wait for the worker to register")
    provider = st.compute.providers.get("standalone")
    t0 = time.monotonic()
    provider.close()
    wait_until(lambda: not _lock_row_exists(st), 10, message="the pool lock to be released after the provider was closed")
    assert time.monotonic() - t0 < 10.0
    wait_until(lambda: st.job(job["id"])["state"] != "provisioning", 30, message="the job to leave provisioning")
