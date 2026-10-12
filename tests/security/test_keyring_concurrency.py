# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The key ring never loses a key: concurrent rotations from threads, processes and (PostgreSQL) several engines are applied one after the other.

The historical bug: version rows are pruned, so an insert-only compare-and-swap is not one - a writer that read version n and was delayed while n+1
and n+2 were written (n+1 pruned) found the key ``n+1`` free again, succeeded, and its key vanished behind n+2.  The ring is now written in one serialized
transaction (advisory lock on PostgreSQL, ``BEGIN IMMEDIATE`` on SQLite).  The tests run on SQLite always and on PostgreSQL when ``OLCF_TEST_POSTGRES_URL``
names a server where the role may ``CREATE DATABASE`` (and ``psycopg`` is importable).
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import textwrap
import threading
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.security.keyring import PREFIX, Ring, unwrap_with, wrap_with

ROUNDS = 20
THREADS = 4
ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("OLCF_TEST_POSTGRES_URL")


class RootWrapper:
    """A fixed root key (what ``MasterKey`` is for the real ring)."""

    key = b"\x07" * 32

    def wrap(self, plaintext: bytes, aad: bytes) -> tuple[str, str]:
        return wrap_with(self.key, plaintext, aad), "root"

    def unwrap(self, blob: str, wrapped_by: str, aad: bytes) -> bytes:  # noqa: ARG002
        return unwrap_with(self.key, blob, aad)

    def wrapper_id(self) -> str:
        return "root"


def kid_of(material: bytes) -> str:
    return hashlib.sha256(material).hexdigest()[:16]


def new_ring(db: Database) -> Ring:
    ring = Ring("master", RootWrapper(), kid_of)
    ring.attach(db)
    return ring


@pytest.fixture(autouse=True)
def fast_ring(monkeypatch):
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "0")


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=[pytest.mark.postgres, pytest.mark.skipif(not PG, reason="OLCF_TEST_POSTGRES_URL not set")])])
def db_url(request, tmp_path):
    if request.param == "sqlite":
        url = f"sqlite:///{tmp_path / 'ring.db'}"
        Database(url).migrate()
        yield url
        return
    pytest.importorskip("psycopg")
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    base = make_url(PG)
    admin = create_engine(base.set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    name = f"olcf_ring_{uuid.uuid4().hex[:10]}"
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    url = base.set(drivername="postgresql+psycopg", database=name).render_as_string(hide_password=False)
    Database(url).migrate()
    try:
        yield url
    finally:
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def ring_rows(db: Database) -> list[str]:
    with db.connect() as conn:
        return [r[0] for r in conn.execute(select(S.kv.c.key).where(S.kv.c.key.like(PREFIX + "master.%")))]


def run_threads(targets: list) -> list[BaseException]:
    errors: list[BaseException] = []
    gate = threading.Barrier(len(targets))

    def wrap(fn):
        def go():
            try:
                gate.wait(10)
                fn()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
        return go

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    [t.start() for t in threads]
    [t.join(120) for t in threads]
    assert not any(t.is_alive() for t in threads), "a rotation hung"
    return errors


def test_threads_sharing_one_ring_never_lose_a_key(db_url):
    db = Database(db_url)
    ring = new_ring(db)
    issued: list[str] = []
    for rnd in range(ROUNDS):
        errors = run_threads([lambda: issued.append(ring.rotate("t", external_current="ext")[0])] * THREADS)
        assert not errors, errors
        doc = ring._load_doc()  # noqa: SLF001
        assert doc["version"] == THREADS * (rnd + 1), f"round {rnd}: a rotation was lost"
        assert len(doc["keys"]) == len(set(issued)) == THREADS * (rnd + 1)
        assert {k["kid"] for k in doc["keys"]} == set(issued) and doc["current"] in set(issued)
    assert len(ring_rows(db)) == 1, "old versions are pruned: exactly one row per ring"


def test_independent_engines_one_per_node_never_lose_a_key(db_url):
    """Several control-plane nodes in one process: separate engines and rings on the same database (no shared in-process lock)."""
    rings = [new_ring(Database(db_url)) for _ in range(THREADS)]
    issued: list[str] = []
    for rnd in range(ROUNDS):
        errors = run_threads([(lambda r=r: issued.append(r.rotate("n", external_current="ext")[0])) for r in rings])
        assert not errors, errors
        doc = rings[0]._load_doc()  # noqa: SLF001
        assert doc["version"] == THREADS * (rnd + 1) and {k["kid"] for k in doc["keys"]} == set(issued), f"round {rnd}: a rotation was lost"
    assert len(ring_rows(rings[0].db)) == 1  # type: ignore[arg-type]


def test_a_stale_writer_cannot_overwrite_pruned_versions(db_url):
    """The deterministic form of the race: a writer holds version n, two others write n+1 and n+2 (pruning n+1); its write of "n+1" must be refused."""
    db = Database(db_url)
    ring = new_ring(db)
    ring.rotate("a", external_current="ext")  # version 1
    stale = ring._load_doc()  # noqa: SLF001
    ring.rotate("b", external_current="ext")  # version 2 (version 1 pruned)
    ring.rotate("c", external_current="ext")  # version 3 (version 2 pruned): key ...00000002 is free again
    assert ring_rows(db) == [f"{PREFIX}master.00000003"]
    forged = dict(stale)
    forged["keys"] = [*stale["keys"], {"kid": "lost", "wrapped": "x", "wrapped_by": "root", "created_at": "", "stage_since": "", "created_by": "stale"}]
    with db.begin_serialized("olcf.keyring.master") as conn:
        assert ring._write(conn, forged, stale["version"]) is False  # noqa: SLF001 - "expected 1" but the ring is at 3
    final = ring._load_doc()  # noqa: SLF001
    assert final["version"] == 3 and len(final["keys"]) == 3 and "lost" not in {k["kid"] for k in final["keys"]}


def test_a_rotation_blocked_behind_another_sees_its_result(db_url):
    """Hold the ring lock open while a second rotation waits: when released the second one builds on the first one's ring (read inside the lock)."""
    db = Database(db_url)
    ring = new_ring(db)
    entered, release = threading.Event(), threading.Event()
    first: list[tuple[str, str | None]] = []
    second: list[tuple[str, str | None]] = []
    original = Ring._write  # noqa: SLF001

    def slow_write(self, conn, doc, expected):
        if not entered.is_set():
            entered.set()
            assert release.wait(30)
        return original(self, conn, doc, expected)

    Ring._write = slow_write  # type: ignore[method-assign]  # noqa: SLF001
    try:
        t1 = threading.Thread(target=lambda: first.append(ring.rotate("one", external_current="ext")))
        t1.start()
        assert entered.wait(30)
        t2 = threading.Thread(target=lambda: second.append(new_ring(Database(db_url)).rotate("two", external_current="ext")))
        t2.start()
        t2.join(1.0)
        assert t2.is_alive(), "the second rotation must wait for the first one to commit"
        release.set()
        t1.join(60)
        t2.join(60)
    finally:
        Ring._write = original  # type: ignore[method-assign]  # noqa: SLF001
    doc = ring._load_doc()  # noqa: SLF001
    assert doc["version"] == 2 and {k["kid"] for k in doc["keys"]} == {first[0][0], second[0][0]} and second[0][1] == first[0][0]


CHILD = textwrap.dedent(
    """
    import sys, time
    from olcf.db.engine import Database
    from tests.security.test_keyring_concurrency import new_ring

    url, count, start_at = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
    ring = new_ring(Database(url))
    while time.time() < start_at:
        time.sleep(0.001)
    for _ in range(count):
        print(ring.rotate("proc", external_current="ext")[0], flush=True)
    """
)


def test_processes_never_lose_a_key(db_url, tmp_path):
    """Separate OS processes (several control-plane nodes / workers on one database), released at the same instant."""
    import time

    db = Database(db_url)
    new_ring(db).rotate("seed", external_current="ext")
    script = tmp_path / "child.py"
    script.write_text(CHILD)
    nproc, each = 4, 5
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT), os.environ.get("PYTHONPATH", "")]), "OLCF_KEYRING_REFRESH_SECONDS": "0"}
    start_at = time.time() + 3
    procs = [subprocess.Popen([sys.executable, str(script), db_url, str(each), str(start_at)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(nproc)]
    outs = []
    try:
        for p in procs:
            out, err = p.communicate(timeout=180)
            assert p.returncode == 0, err
            outs.append(out.split())
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
                p.wait()
    kids = [k for o in outs for k in o]
    assert len(kids) == nproc * each == len(set(kids))
    doc = new_ring(db)._load_doc()  # noqa: SLF001
    assert doc["version"] == 1 + nproc * each and len(doc["keys"]) == 1 + nproc * each
    assert set(kids) <= {k["kid"] for k in doc["keys"]}
    assert len(ring_rows(db)) == 1
