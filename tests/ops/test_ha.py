# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""High availability: two real control-plane processes (uvicorn) on one PostgreSQL database.

Needs ``OLCF_TEST_POSTGRES_URL`` (a server where the role may CREATE DATABASE) and ``psycopg``. Checks that

* exactly one replica holds the scheduler lease and the other is a follower, at all times;
* both replicas serve the same data at once (a write through one is visible through the other) and accept each other's tokens;
* when the leader is killed (SIGKILL, no goodbye) the survivor takes over within the lease TTL (15 s) plus one renewal period, keeps serving, and loses no metadata;
* a graceful stop hands the lease over immediately; a restarted replica rejoins as follower; the audit chain stays intact throughout;
* a leader that can no longer reach the database stops acting as leader when its lease expires (clock fencing).
"""

from __future__ import annotations

import base64
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

PG = os.environ.get("OLCF_TEST_POSTGRES_URL")
pytestmark = [pytest.mark.postgres, pytest.mark.skipif(not PG, reason="OLCF_TEST_POSTGRES_URL not set")]

HERE = Path(__file__).parent
LEASE_TTL = 15.0
RENEW = LEASE_TTL / 3
TAKEOVER_BUDGET = LEASE_TTL + RENEW + 8.0  # expiry + the survivor's next renewal tick + scheduling slack
ADMIN_PASSWORD = "Ha-Test-Admin-Password-1"  # noqa: S105 - fixture credential, not a secret


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    def __init__(self, name: str, root: Path, db_url: str, env: dict[str, str]) -> None:
        self.name, self.port = name, free_port()
        self.data = root / name
        self.data.mkdir(parents=True, exist_ok=True)
        self.env = {**env, "OLCF_DB__URL": db_url}
        self.proc: subprocess.Popen[bytes] | None = None
        self.log = (root / f"{name}.log").open("ab")
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=15, trust_env=False)

    def start(self) -> None:
        self.proc = subprocess.Popen([sys.executable, "-u", str(HERE / "hanode.py"), str(self.data), str(self.port)], env=self.env, stdout=self.log, stderr=subprocess.STDOUT,
                                     cwd=str(HERE.parent.parent))
        end = time.time() + 90
        while time.time() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited with {self.proc.returncode}:\n{self.tail()}")
            try:
                if self.client.get("/v1/health/ready").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{self.name} did not become ready:\n{self.tail()}")

    def tail(self) -> str:
        self.log.flush()
        return Path(self.log.name).read_text(errors="replace")[-3000:]

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(10)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                self.kill()

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def is_leader(self) -> bool | None:
        try:
            r = self.client.get("/v1/health/ready")
            return bool(r.json().get("leader")) if r.status_code == 200 else None
        except (httpx.HTTPError, ValueError):
            return None

    def login(self) -> dict[str, str]:
        r = self.client.post("/v1/identity/login", json={"username": "admin", "password": ADMIN_PASSWORD})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    pytest.importorskip("psycopg")
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    base = make_url(PG)
    admin = create_engine(base.set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    dbname = f"olcf_ha_{uuid.uuid4().hex[:8]}"
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{dbname}"'))
    url = base.set(drivername="postgresql+psycopg", database=dbname).render_as_string(hide_password=False)
    root = tmp_path_factory.mktemp("ha")
    env = {**os.environ, "OLCF_MASTER_KEY": base64.b64encode(os.urandom(32)).decode(), "OLCF_JWT_SIGNING_KEY": base64.urlsafe_b64encode(os.urandom(48)).decode(),
           "OLCF_BOOTSTRAP_ADMIN_PASSWORD": ADMIN_PASSWORD, "PYTHONPATH": os.pathsep.join(["src", ".", "/tmp/pylib", os.environ.get("PYTHONPATH", "")]), "OLCF_LOG_LEVEL": "INFO",  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
           "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    a, b = Node("a", root, url, env), Node("b", root, url, env)
    try:
        a.start()
        b.start()
        yield a, b, url
    finally:
        for n in (a, b):
            n.kill()
            n.client.close()
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)'))
        admin.dispose()


def leaders(*nodes: Node) -> list[Node]:
    return [n for n in nodes if n.alive and n.is_leader()]


def wait_until(cond, timeout: float, step: float = 0.25):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(step)
    return cond()


def test_exactly_one_leader_and_shared_state(cluster):
    a, b, _url = cluster
    assert wait_until(lambda: len(leaders(a, b)) == 1, 30), "exactly one replica must hold the lease"
    for _ in range(12):  # never two leaders, not even briefly
        assert len(leaders(a, b)) <= 1
        time.sleep(0.25)
    ha, hb = a.login(), b.login()
    # tokens are interchangeable between replicas (shared signing key), data is shared
    assert b.client.get("/v1/identity/me", headers=ha).status_code == 200
    w = a.client.post("/v1/workspaces", json={"name": "ha-ws", "description": "created on a"}, headers=ha)
    assert w.status_code == 201, w.text
    got = b.client.get(f"/v1/workspaces/{w.json()['id']}", headers=hb)
    assert got.status_code == 200 and got.json()["description"] == "created on a"
    it = b.client.post(f"/v1/workspaces/{w.json()['id']}/items", json={"type": "variable_set", "name": "from-b", "definition": {"variables": []}}, headers=hb)
    assert it.status_code == 201, it.text
    assert a.client.get(f"/v1/items/{it.json()['id']}", headers=ha).json()["name"] == "from-b"
    sec = a.client.post("/v1/secrets", json={"name": "ha-secret", "value": "HA-SECRET-VALUE-123456", "scope_type": "workspace", "scope_id": w.json()["id"]}, headers=ha)
    assert sec.status_code == 201
    # the secret is encrypted with the shared master key: the other replica can use it (jobs resolve it just in time)
    job = b.client.post("/v1/jobs", json={"kind": "script", "workspace_id": w.json()["id"], "engine": "python", "source": {"format": "py", "content": "print(1)", "filename": "x.py"},
                                          "secret_refs": {"S": sec.json()["id"]}}, headers=hb)
    assert job.status_code == 202, job.text
    health = a.client.get("/v1/admin/health/checks", headers=ha)
    assert health.status_code == 200
    ver = a.client.get("/v1/audit/verify", headers=ha).json()
    assert ver["ok"] is True


def test_leader_failover_within_the_lease_ttl(cluster):
    a, b, _url = cluster
    assert wait_until(lambda: len(leaders(a, b)) == 1, 30)
    lead = leaders(a, b)[0]
    follower = b if lead is a else a
    hf = follower.login()
    before = follower.client.get("/v1/workspaces", headers=hf).json()
    ws_ids = {w["id"] for w in before["items"]}
    assert ws_ids, "state to survive the failover"
    t0 = time.time()
    lead.kill()
    # the API of the survivor never goes away
    ok_during = 0
    took = None
    while time.time() - t0 < TAKEOVER_BUDGET + 10:
        r = follower.client.get("/v1/workspaces", headers=hf)
        assert r.status_code == 200, f"the surviving replica must stay available (got {r.status_code})"
        ok_during += 1
        if follower.is_leader():
            took = time.time() - t0
            break
        time.sleep(0.5)
    assert took is not None, "the survivor never became leader"
    print(f"HA takeover after SIGKILL of the leader: {took:.1f}s (lease ttl {LEASE_TTL}s)")
    assert took <= TAKEOVER_BUDGET, f"takeover took {took:.1f}s, budget {TAKEOVER_BUDGET:.1f}s (lease ttl {LEASE_TTL}s)"
    assert ok_during >= 3
    # metadata intact, writes work, the audit chain verifies, new logins work
    after = follower.client.get("/v1/workspaces", headers=hf).json()
    assert {w["id"] for w in after["items"]} >= ws_ids
    w = follower.client.post("/v1/workspaces", json={"name": f"after-failover-{uuid.uuid4().hex[:6]}"}, headers=hf)
    assert w.status_code == 201
    assert follower.client.get("/v1/audit/verify", headers=hf).json()["ok"] is True
    follower.login()
    info = follower.client.get("/v1/admin/info", headers=hf).json()
    assert info["leader"] is True
    # singleton work runs on the new leader (the ops tick is leader-only)
    ops = follower.client.get("/v1/admin/retention", headers=hf)
    assert ops.status_code == 200
    # restart the dead replica: it rejoins as follower and the survivor keeps the lease
    lead.start()
    time.sleep(RENEW * 2 + 1)
    assert leaders(a, b) == [follower], "the restarted replica must not steal a healthy lease"
    assert lead.client.get("/v1/identity/me", headers=hf).status_code == 200


def test_graceful_stop_hands_over_quickly(cluster):
    a, b, _url = cluster
    assert wait_until(lambda: len(leaders(a, b)) == 1, 30)
    lead = leaders(a, b)[0]
    other = b if lead is a else a
    t0 = time.time()
    lead.stop()
    took = wait_until(lambda: other.is_leader() and (time.time() - t0), RENEW + 6)
    assert took, "a gracefully stopped leader must release the lease"
    print(f"HA graceful hand-over: {took:.1f}s")
    assert took < LEASE_TTL, f"graceful hand-over took {took:.1f}s"
    lead.start()
    assert wait_until(lambda: lead.is_leader() is False, 20), "the returning replica is a follower"
    assert len(leaders(a, b)) == 1


def test_both_replicas_down_then_recovery_keeps_all_metadata(cluster):
    a, b, url = cluster
    ha = a.login()
    names_before = {w["name"] for w in a.client.get("/v1/workspaces", headers=ha).json()["items"]}
    a.kill()
    b.kill()
    a.start()
    ha = a.login()
    assert {w["name"] for w in a.client.get("/v1/workspaces", headers=ha).json()["items"]} == names_before
    assert wait_until(lambda: a.is_leader(), 30), "a lone replica becomes leader after the old lease expires"
    b.start()
    assert wait_until(lambda: len(leaders(a, b)) == 1, 30)
    assert a.client.get("/v1/audit/verify", headers=ha).json()["ok"] is True


def test_partitioned_leader_fences_itself(cluster):
    """The lease clock, not the last answer from the database, decides: a leader that cannot renew stops being leader when the lease would expire for others."""
    from olcf.core.leader import LeaderElector

    class FakeDb:
        def __init__(self, real) -> None:
            self.real, self.down = real, False

        def begin(self):
            if self.down:
                raise RuntimeError("database unreachable")
            return self.real.begin()

        def connect(self):
            if self.down:
                raise RuntimeError("database unreachable")
            return self.real.connect()

    _a, _b, url = cluster
    from olcf.db.engine import Database

    db = FakeDb(Database(url))
    el = LeaderElector(db, f"test-fence-{uuid.uuid4().hex[:6]}", "holder-x", ttl=2.0)
    assert el.acquire() and el.is_leader()
    db.down = True
    with pytest.raises(RuntimeError):
        el.acquire()
    assert el.is_leader(), "within the lease the holder may still act"
    time.sleep(2.2)
    assert not el.is_leader(), "after the lease ran out the holder must stop acting as leader even though it never heard from the database"
