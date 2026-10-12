# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Backup/restore on PostgreSQL (set OLCF_TEST_POSTGRES_URL to a server where the role may CREATE DATABASE) and across dialects."""

from __future__ import annotations

import os
import sys
import uuid

import pytest
from sqlalchemy import select, text

from olcf.db import schema as S
from olcf.ops import backup_core as core
from olcf.ops.crypto import KeySpec
from tests.ops.conftest import PASSPHRASE

if "/tmp/pylib" not in sys.path:  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
    sys.path.append("/tmp/pylib")  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location

PG = os.environ.get("OLCF_TEST_POSTGRES_URL")
pytestmark = [pytest.mark.postgres, pytest.mark.skipif(not PG, reason="OLCF_TEST_POSTGRES_URL not set")]


@pytest.fixture()
def pg_factory():
    pytest.importorskip("psycopg")
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url

    base = make_url(PG)
    admin = create_engine(base.set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    made: list[str] = []

    def make() -> str:
        name = f"olcf_t_{uuid.uuid4().hex[:10]}"
        with admin.connect() as c:
            c.execute(text(f'CREATE DATABASE "{name}"'))
        made.append(name)
        return base.set(database=name).render_as_string(hide_password=False)

    yield make
    with admin.connect() as c:
        for n in made:
            c.execute(text(f'DROP DATABASE IF EXISTS "{n}" WITH (FORCE)'))
    admin.dispose()


def _ctx(settings, url):
    from olcf.api.app import install_modules
    from olcf.core.context import build_context

    settings.db.url = url
    c = build_context(settings)
    c.modules = install_modules(c, ["olcf.api.core.module", "olcf.ops.module"])
    return c


def _populate(ctx):
    for i in range(3):
        ctx.secrets.create(f"s{i}", f"value-{i}")
    for i in range(30):
        ctx.audit.record(None, "t.a", resource_type="x", resource_id=str(i), details={"i": i, "uni": "café"})
    for i in range(5):
        ctx.events.publish("t.e", {"i": i}, persist=True)


def _dump_tables(db, names):
    out = {}
    with db.connect() as conn:
        for n in names:
            t = S.metadata.tables[n]
            out[n] = [dict(r._mapping) for r in conn.execute(select(t).order_by(*t.primary_key.columns))]
    return out


def test_pg_roundtrip_with_sequences(settings, pg_factory, tmp_path):
    src = _ctx(settings, pg_factory())
    try:
        _populate(src)
        res = core.write_backup(src.db, tmp_path / "pg.olcfbak", KeySpec(PASSPHRASE), meta={"backup_id": "bkp_pg"})
        assert res.rows > 40
        assert core.validate_backup(tmp_path / "pg.olcfbak", KeySpec(PASSPHRASE)).ok
        from olcf.db.engine import Database

        tgt = Database(pg_factory())
        rep = core.restore_backup(tmp_path / "pg.olcfbak", KeySpec(PASSPHRASE), tgt)
        assert rep.ok
        cmp = ["secrets", "audit_log", "events", "principals", "role_bindings", "kv", "api_tokens"]
        assert _dump_tables(src.db, cmp) == _dump_tables(tgt, cmp)
        # PG sequences were advanced: new autoincrement inserts do not collide
        from olcf.core.events import EventBus
        from olcf.security.audit import AuditLog

        al = AuditLog(tgt)
        al.record(None, "after.restore")
        al.record(None, "after.restore2")
        assert al.verify()["ok"]
        EventBus(tgt).publish("after.restore", {}, persist=True)
        # restore again over existing data without wipe is refused, with wipe it works
        from olcf.core.errors import Conflict

        with pytest.raises(Conflict):
            core.restore_backup(tmp_path / "pg.olcfbak", KeySpec(PASSPHRASE), tgt, wipe=False)
        assert core.restore_backup(tmp_path / "pg.olcfbak", KeySpec(PASSPHRASE), tgt, wipe=True).ok
        tgt.dispose()
    finally:
        src.db.dispose()


def test_cross_dialect_pg_to_sqlite_and_back(settings, pg_factory, tmp_path):
    from olcf.db.engine import Database

    src = _ctx(settings, pg_factory())
    try:
        _populate(src)
        core.write_backup(src.db, tmp_path / "x.olcfbak", KeySpec(PASSPHRASE), meta={})
        lite = Database(f"sqlite:///{tmp_path / 'lite.db'}")
        assert core.restore_backup(tmp_path / "x.olcfbak", KeySpec(PASSPHRASE), lite).ok
        cmp = ["secrets", "audit_log", "kv"]
        assert _dump_tables(src.db, cmp) == _dump_tables(lite, cmp)
        core.write_backup(lite, tmp_path / "y.olcfbak", KeySpec(PASSPHRASE), meta={})
        back = Database(pg_factory())
        assert core.restore_backup(tmp_path / "y.olcfbak", KeySpec(PASSPHRASE), back).ok
        assert _dump_tables(src.db, cmp) == _dump_tables(back, cmp)
        lite.dispose()
        back.dispose()
    finally:
        src.db.dispose()


def test_pg_snapshot_is_consistent_under_concurrent_writes(settings, pg_factory, tmp_path):
    import threading

    src = _ctx(settings, pg_factory())
    try:
        _populate(src)
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                src.audit.record(None, "concurrent", resource_id=str(i))
                i += 1

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        try:
            core.write_backup(src.db, tmp_path / "c.olcfbak", KeySpec(PASSPHRASE), meta={}, batch=50)
        finally:
            stop.set()
            t.join()
        from olcf.db.engine import Database
        from olcf.security.audit import AuditLog

        tgt = Database(pg_factory())
        core.restore_backup(tmp_path / "c.olcfbak", KeySpec(PASSPHRASE), tgt)
        assert AuditLog(tgt).verify()["ok"], "the restored audit chain must be internally consistent"
        tgt.dispose()
    finally:
        src.db.dispose()


def test_ops_service_on_pg(settings, pg_factory):
    """The whole service (backup + restore-test, which restores into SQLite) driven against PostgreSQL."""
    from olcf.security.rbac import Principal

    ctx = _ctx(settings, pg_factory())
    try:
        _populate(ctx)
        ident = ctx.service("identity")
        row = ident.create_principal("pgadmin", kind="user", password="correct horse battery staple")  # noqa: S106 - fixture credential, not a secret
        ident.bind(row["id"], "PlatformAdmin", scope_type="org", scope_id="")
        svc = ctx.service("backup")
        admin = Principal(id=row["id"], name="pgadmin")
        out = svc.start_backup(admin, wait=True)
        rec = svc.get(out["backup_id"])
        assert rec["state"] == "succeeded", rec
        assert rec["dialect"] == "postgresql"
        res = svc.restore_test(admin, rec["id"])
        assert res["ok"], [c for c in res["checks"] if not c["ok"]]
    finally:
        ctx.db.dispose()


def test_online_restore_on_pg_replaces_live_data(settings, pg_factory):
    from olcf.security.rbac import Principal

    ctx = _ctx(settings, pg_factory())
    try:
        ident = ctx.service("identity")
        row = ident.create_principal("pgadmin2", kind="user", password="correct horse battery staple")  # noqa: S106 - fixture credential, not a secret
        ident.bind(row["id"], "PlatformAdmin", scope_type="org", scope_id="")
        admin = Principal(id=row["id"], name="pgadmin2")
        ctx.secrets.create("keep", "v1")
        svc = ctx.service("backup")
        bid = svc.start_backup(admin, wait=True)["backup_id"]
        ctx.secrets.create("drop-me", "v2")
        ctx.audit.record(None, "after.backup")
        out = svc.start_restore(admin, bid, passphrase=None, confirm="RESTORE", wait=True)
        op = ctx.service("operations").get(out["operation_id"])
        assert op["state"] == "succeeded", op
        with ctx.db.connect() as conn:
            names = {r[0] for r in conn.execute(select(S.secrets.c.name))}
        assert "keep" in names and "drop-me" not in names
        ctx.audit.record(None, "post.restore.insert")  # sequence advanced
        assert ctx.audit.verify()["ok"]
    finally:
        ctx.db.dispose()
