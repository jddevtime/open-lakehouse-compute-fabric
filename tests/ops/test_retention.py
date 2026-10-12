# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Retention and housekeeping: every purge step, dry runs, audit archive + hash-chain anchors, recycle bin, authorisation."""

from __future__ import annotations

import gzip
import hashlib
import json
from contextlib import contextmanager

from sqlalchemy import Column, String, Table, delete, func, insert, select, update

from olcf.core.ids import new_id
from olcf.core.timeutil import iso_in, now_iso
from olcf.db import schema as S
from olcf.ops import schema as O

DAY = 86400


def ago(days: float) -> str:
    return iso_in(-days * DAY)


@contextmanager
def clock(monkeypatch, ts: str):
    with monkeypatch.context() as m:
        m.setattr("olcf.security.audit.now_iso", lambda: ts)
        yield


def count(ctx, table, *where) -> int:
    with ctx.db.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(table).where(*where)).scalar())


def put(c, key: str, value: int):
    r = c.put(f"/v1/admin/settings/{key}", json={"value": value}, headers=c.admin)
    return r


def seed_audit(c, monkeypatch, old: int, new: int) -> None:
    """``old`` records stamped now, then ``new`` records stamped a year ahead, with the retention cutoff moved 200 days forward:
    the audit log is append-only and time-ordered, so "old" must precede "new"."""
    import olcf.ops.retention as R

    assert put(c, "retention.audit_days", 30).status_code == 200
    for i in range(old):
        c.ctx.audit.record(None, "old.action", resource_id=str(i), details={"i": i})
    real = R.iso_in
    monkeypatch.setattr(R, "iso_in", lambda s: real(s + 200 * DAY))
    for i in range(new):
        with clock(monkeypatch, real(365 * DAY)):
            c.ctx.audit.record(None, "new.action", resource_id=str(i))


# ----------------------------------------------------------------------------- settings
def test_settings_registered_and_bounded(ops_app):
    c = ops_app
    allv = c.get("/v1/admin/settings", headers=c.admin).json()
    names = set(allv["settings"])
    assert {"retention.audit_days", "retention.event_days", "retention.recycle_bin_days", "retention.enabled"} <= names
    assert put(c, "retention.audit_days", 5).status_code == 400  # audit may not be shortened below 30 days
    assert put(c, "retention.audit_days", 90).status_code == 200
    assert put(c, "retention.event_days", 0).status_code == 400
    assert put(c, "retention.event_days", "7").status_code == 400
    assert put(c, "retention.event_days", True).status_code == 400
    assert c.ctx.service("retention").setting("audit_days") == 90
    # defaults come from the static configuration
    assert c.ctx.service("retention").setting("metric_days") == c.ctx.settings.retention.metric_days


# ----------------------------------------------------------------------------- audit
def test_audit_purge_archive_and_anchor(ops_app, monkeypatch):
    c = ops_app
    ctx = c.ctx
    seed_audit(c, monkeypatch, old=30, new=5)
    svc = ctx.service("retention")
    total_before = count(ctx, S.audit_log)
    v0 = svc.verify_audit()
    assert v0["ok"] and v0["anchors"] == 0
    # dry run changes nothing
    dry = c.post("/v1/admin/retention/run", json={"only": ["audit"]}, headers=c.admin).json()
    assert dry["dry_run"] is True and dry["steps"]["audit"]["candidates"] >= 30 and dry["steps"]["audit"]["deleted"] == 0
    assert count(ctx, S.audit_log, S.audit_log.c.action == "old.action") == 30 and count(ctx, S.audit_log) <= total_before + 1 and count(ctx, O.audit_anchors) == 0
    # real run needs admin.settings; the platform admin has it
    rep = c.post("/v1/admin/retention/run", json={"dry_run": False, "only": ["audit"]}, headers=c.admin).json()
    st = rep["steps"]["audit"]
    assert st["deleted"] >= 30 and st["archive"]["records"] == st["deleted"]
    left = count(ctx, S.audit_log)
    assert left >= 5  # the recent records and the run's own audit entry stay
    assert count(ctx, S.audit_log, S.audit_log.c.action == "old.action") == 0
    assert count(ctx, S.audit_log, S.audit_log.c.action == "new.action") == 5
    # the archive file is complete and matches its recorded digest
    arch = ctx.settings.path("archive", "audit") / st["archive"]["file"]
    raw = arch.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == st["archive"]["sha256"]
    recs = [json.loads(line) for line in gzip.decompress(raw).decode().splitlines()]
    assert len(recs) == st["archive"]["records"] and recs[0]["seq"] < recs[-1]["seq"] and all(r["action"] != "new.action" for r in recs)
    # the hash chain across the cut: archive + remaining records form one chain
    v = svc.verify_audit()
    assert v["ok"] and v["anchors"] == 1 and v["purged_records"] == st["deleted"] and v["anchored_to"]["archive_sha256"] == st["archive"]["sha256"]
    with ctx.db.connect() as conn:
        anchor = conn.execute(select(O.audit_anchors)).first()._mapping
        first = conn.execute(select(S.audit_log).order_by(S.audit_log.c.seq).limit(1)).first()._mapping
    assert anchor["last_purged_hash"] == first["prev_hash"]
    assert recs[-1]["hash"] == anchor["last_purged_hash"]
    # a second purge extends the same trail; records are never lost silently
    assert svc.run(None, only=["audit"])["steps"]["audit"]["deleted"] == 0


def test_audit_truncation_without_anchor_is_detected(ops_app, monkeypatch):
    c = ops_app
    ctx = c.ctx
    seed_audit(c, monkeypatch, old=10, new=5)
    svc = ctx.service("retention")
    assert svc.verify_audit()["ok"]
    # an attacker (or a bug) deletes the oldest records directly: AuditLog.verify alone cannot tell, the anchored verification can
    from olcf.security.audit import AuditLog

    with ctx.db.begin() as conn:
        seqs = [r[0] for r in conn.execute(select(S.audit_log.c.seq).order_by(S.audit_log.c.seq).limit(6)).all()]
        conn.execute(delete(S.audit_log).where(S.audit_log.c.seq.in_(seqs)))
    assert AuditLog(ctx.db).verify()["ok"] is True  # the weakness the anchors close
    v = svc.verify_audit()
    assert v["ok"] is False and "not covered by any retention anchor" in v["reason"]


def test_audit_tamper_after_anchor_is_detected(ops_app, monkeypatch):
    c = ops_app
    ctx = c.ctx
    seed_audit(c, monkeypatch, old=10, new=5)
    svc = ctx.service("retention")
    svc.run(None, only=["audit"])
    assert svc.verify_audit()["ok"]
    with ctx.db.begin() as conn:  # rewrite history behind the anchor
        conn.execute(update(O.audit_anchors).values(last_purged_hash="0" * 64))
    assert svc.verify_audit()["ok"] is False
    with ctx.db.begin() as conn:
        conn.execute(update(O.audit_anchors).values(last_purged_hash=conn.execute(select(S.audit_log.c.prev_hash).order_by(S.audit_log.c.seq).limit(1)).scalar()))
        conn.execute(update(S.audit_log).where(S.audit_log.c.action == "new.action", S.audit_log.c.resource_id == "2").values(actor_name="mallory"))
    v = svc.verify_audit()
    assert v["ok"] is False and v["broken_at_seq"]


def test_audit_always_keeps_head_and_respects_archive_switch(ops_app, monkeypatch):
    c = ops_app
    ctx = c.ctx
    seed_audit(c, monkeypatch, old=8, new=0)
    # even if everything is older than the cutoff the newest record stays so the chain keeps its head
    assert put(c, "retention.audit_archive", 0).status_code == 200
    svc = ctx.service("retention")
    st = svc.run(None, only=["audit"])["steps"]["audit"]
    assert st["deleted"] >= 7 and "archive" not in st
    assert count(ctx, S.audit_log) >= 1
    assert svc.verify_audit()["ok"]
    ctx.audit.record(None, "after.purge")  # chain continues from the surviving head
    assert svc.verify_audit()["ok"]
    assert not list(ctx.settings.path("archive", "audit").glob("*.gz"))


# ----------------------------------------------------------------------------- simple tables
def test_events_tokens_idempotency_operations_runs(ops_app):
    c = ops_app
    ctx = c.ctx
    svc = ctx.service("retention")
    with ctx.db.begin() as conn:
        for _ in range(12):
            conn.execute(insert(S.events).values(id=new_id("evt"), ts=ago(40), type="old.event", payload={}))
        conn.execute(insert(S.events).values(id=new_id("evt"), ts=ago(1), type="new.event", payload={}))
        admin_id = conn.execute(select(S.principals.c.id).limit(1)).scalar()

        def tok(name, expires, revoked):
            conn.execute(insert(S.api_tokens).values(id=new_id("tok"), principal_id=admin_id, kind="pat", name=name, prefix=new_id("tok")[:20], token_hash="x", scopes=[], meta={},  # noqa: S106 - fixture credential, not a secret
                                                     created_at=ago(100), expires_at=expires, revoked_at=revoked))

        tok("expired-long-ago", ago(30), None)
        tok("expired-yesterday", ago(1), None)
        tok("revoked-long-ago", None, ago(30))
        tok("active", iso_in(DAY), None)
        tok("never-expires", None, None)
        for k, exp in (("k-old", ago(1)), ("k-new", iso_in(3600))):
            conn.execute(insert(S.idempotency_keys).values(key=k, principal_id=admin_id, endpoint="/x", request_hash="h", created_at=ago(2), expires_at=exp))

        def op(state, updated):
            oid = new_id("evt").replace("evt_", "ops_")
            conn.execute(insert(S.operations).values(id=oid, kind="x", state=state, progress=0, created_at=updated, updated_at=updated))
            return oid

        stale = op("running", ago(3))
        live = op("running", now_iso())
        old_done = op("succeeded", ago(60))
        new_done = op("succeeded", ago(1))
    rep = svc.run(None, dry_run=True)
    s = rep["steps"]
    assert s["events"]["candidates"] == 12 and s["tokens"]["candidates"] == 2 and s["idempotency"]["candidates"] == 1
    assert s["operations"]["stale_candidates"] == 1 and s["operations"]["candidates"] == 1
    assert count(ctx, S.events, S.events.c.type == "old.event") == 12 and count(ctx, S.api_tokens) >= 5  # dry run: untouched
    with ctx.db.connect() as conn:
        assert conn.execute(select(S.operations.c.state).where(S.operations.c.id == stale)).scalar() == "running"
    rep = svc.run(None)
    s = rep["steps"]
    assert s["events"]["deleted"] == 12 and s["tokens"]["deleted"] == 2 and s["idempotency"]["deleted"] == 1 and s["operations"]["marked_failed"] == 1 and s["operations"]["deleted"] == 1
    assert count(ctx, S.events, S.events.c.type == "old.event") == 0 and count(ctx, S.events, S.events.c.type == "new.event") == 1
    with ctx.db.connect() as conn:
        names = {r[0] for r in conn.execute(select(S.api_tokens.c.name)).all()}
    assert {"expired-yesterday", "active", "never-expires"} <= names and not {"expired-long-ago", "revoked-long-ago"} & names
    assert count(ctx, S.idempotency_keys) == 1
    with ctx.db.connect() as conn:
        row = dict(conn.execute(select(S.operations).where(S.operations.c.id == stale)).first()._mapping)
        assert row["state"] == "failed" and row["error"]["code"] == "abandoned"
        assert conn.execute(select(S.operations.c.state).where(S.operations.c.id == live)).scalar() == "running"
        assert conn.execute(select(S.operations.c.id).where(S.operations.c.id == old_done)).first() is None
        assert conn.execute(select(S.operations.c.id).where(S.operations.c.id == new_done)).first() is not None
    # reports are kept and listed; old reports are pruned by run_history_days
    runs = c.get("/v1/admin/retention/runs", headers=c.admin).json()["items"]
    assert len(runs) >= 2 and {r["dry_run"] for r in runs} == {True, False}
    one = c.get(f"/v1/admin/retention/runs/{rep['id']}", headers=c.admin).json()
    assert one["report"]["total_deleted"] == rep["total_deleted"] and one["error"] is None
    assert c.get("/v1/admin/retention/runs/rtn_nope", headers=c.admin).status_code == 404


def test_unknown_step_and_concurrent_run(ops_app):
    c = ops_app
    assert c.post("/v1/admin/retention/run", json={"only": ["everything"]}, headers=c.admin).status_code == 400
    svc = c.ctx.service("retention")
    assert svc._lock.acquire(blocking=False)
    try:
        r = c.post("/v1/admin/retention/run", json={}, headers=c.admin)
        assert r.status_code == 409
    finally:
        svc._lock.release()
    # another node holding the run lock blocks as well
    from olcf.core.leader import LeaderElector

    other = LeaderElector(c.ctx.db, "ops.retention.run", "node-b", ttl=60)
    assert other.acquire()
    assert c.post("/v1/admin/retention/run", json={}, headers=c.admin).status_code == 409
    other.release()
    assert c.post("/v1/admin/retention/run", json={}, headers=c.admin).status_code == 200


# ----------------------------------------------------------------------------- recycle bin
def test_recycle_bin_items_and_workspaces(ops_app):
    c = ops_app
    ctx = c.ctx
    svc = ctx.service("retention")

    def ws(name):
        r = c.post("/v1/workspaces", json={"name": name}, headers=c.admin)
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def item(wid, name):
        r = c.post(f"/v1/workspaces/{wid}/items", json={"type": "notebook", "name": name, "definition": {"cells": []}}, headers=c.admin)
        assert r.status_code == 201, r.text
        return r.json()["id"]

    keep, gone, blocked = ws("keep-me"), ws("purge-me"), ws("blocked-me")
    i_old, i_new, i_live = item(keep, "old"), item(keep, "new"), item(keep, "live")
    i_in_gone = item(gone, "inside")
    for iid in (i_old, i_new):
        assert c.delete(f"/v1/items/{iid}", headers=c.admin).status_code == 204
    for w in (gone, blocked):
        assert c.delete(f"/v1/workspaces/{w}", headers=c.admin).status_code == 204
    old = ago(45)
    ref = Table("zz_retention_ref", S.metadata, Column("id", String(64), primary_key=True), Column("workspace_id", String(64)))
    try:
        ref.create(ctx.db.engine)
        with ctx.db.begin() as conn:
            conn.execute(update(S.items).where(S.items.c.id.in_([i_old])).values(deleted_at=old))
            conn.execute(update(S.workspaces).where(S.workspaces.c.id.in_([gone, blocked])).values(deleted_at=old))
            conn.execute(insert(ref).values(id="r1", workspace_id=blocked))
            conn.execute(insert(S.item_versions).values(id=new_id("itm"), item_id=i_old, version=9, definition={}, created_at=old))
        # item in the deleted workspace gets soft-deleted too (as the workspace's content), then ages out together
        rep = svc.run(None, dry_run=True, only=["recycle_bin"])
        rb = rep["steps"]["recycle_bin"]
        assert rb["items"]["candidates"] == 1 and rb["workspaces"]["candidates"] == 2 and rb["workspaces"]["would_delete"] == 1
        assert rb["workspaces"]["blocked"][0]["workspace_id"] == blocked and rb["workspaces"]["blocked"][0]["referenced_by"] == {"zz_retention_ref": 1}
        rep = svc.run(None, only=["recycle_bin"])["steps"]["recycle_bin"]
        assert rep["items"]["deleted"] == 1 and rep["workspaces"]["deleted"] == 1
        with ctx.db.connect() as conn:
            ids = {r[0] for r in conn.execute(select(S.items.c.id)).all()}
            wss = {r[0] for r in conn.execute(select(S.workspaces.c.id)).all()}
            assert conn.execute(select(func.count()).select_from(S.item_versions).where(S.item_versions.c.item_id == i_old)).scalar() == 0
        assert i_old not in ids and i_new in ids and i_live in ids and i_in_gone not in ids  # recent deletions stay restorable
        assert gone not in wss and blocked in wss and keep in wss
        assert c.post(f"/v1/items/{i_new}/restore", headers=c.admin).status_code in (200, 201)
        # once the reference disappears the blocked workspace goes too
        with ctx.db.begin() as conn:
            conn.execute(delete(ref))
        assert svc.run(None, only=["recycle_bin"])["steps"]["recycle_bin"]["workspaces"]["deleted"] == 1
    finally:
        ref.drop(ctx.db.engine, checkfirst=True)
        S.metadata.remove(ref)


# ----------------------------------------------------------------------------- other modules
def test_metrics_jobs_and_job_logs(ret_app):
    c = ret_app
    ctx = c.ctx
    from olcf.compute import schema as C

    svc = ctx.service("retention")
    wid = c.post("/v1/workspaces", json={"name": "jobs-ws"}, headers=c.admin).json()["id"]
    from olcf.observability.schema import metric_samples

    with ctx.db.begin() as conn:
        for i in range(5):
            conn.execute(insert(metric_samples).values(ts=ago(30), resolution=15, name="m", labels_key=f"k{i}", labels={}, value=1.0, n=1))
        conn.execute(insert(metric_samples).values(ts=ago(0.5), resolution=15, name="m", labels_key="fresh", labels={}, value=1.0, n=1))

        def job(state, finished_days):
            jid = new_id("job")
            t = ago(finished_days)
            conn.execute(insert(C.jobs).values(id=jid, kind="batch", state=state, workspace_id=wid, created_at=t, updated_at=t, queued_at=t, finished_at=t if state in ("succeeded", "failed") else None))
            return jid

        old_job, new_job, running_old = job("succeeded", 400), job("succeeded", 2), job("running", 500)
    logs = ctx.service("compute").logs
    import os

    for jid in (old_job, new_job, running_old):
        logs.append(jid, "stdout", 0, "hello\n")
    ancient = __import__("time").time() - 200 * DAY
    for jid in (old_job, running_old):
        for p in [logs.job_dir(jid), *logs.job_dir(jid).rglob("*")]:
            os.utime(p, (ancient, ancient))
    rep = svc.run(None, dry_run=True, only=["metrics", "jobs", "job_logs"])["steps"]
    assert rep["metrics"]["candidates"] == 5 and rep["jobs"]["candidates"] == 1 and rep["job_logs"]["candidates"] == 1  # the running job's logs are protected
    assert logs.exists(old_job)
    rep = svc.run(None, only=["job_logs"])["steps"]
    assert rep["job_logs"]["deleted"] == 1 and not logs.exists(old_job) and logs.exists(running_old)  # a running job's logs are never purged
    rep = svc.run(None, only=["metrics", "jobs"])["steps"]
    assert rep["metrics"]["deleted"] == 5 and rep["jobs"]["deleted"] == 1
    with ctx.db.connect() as conn:
        left = {r[0] for r in conn.execute(select(C.jobs.c.id)).all()}
    assert left == {new_job, running_old}
    assert not logs.exists(old_job) and logs.exists(new_job) and logs.exists(running_old)
    assert count(ctx, metric_samples) == 1


def test_missing_modules_are_skipped_not_errors(ops_app):
    rep = ops_app.ctx.service("retention").run(None, dry_run=True)
    assert rep["steps"]["jobs"]["skipped"] and rep["steps"]["job_logs"]["skipped"]
    assert not any("error" in s for s in rep["steps"].values())


def test_failing_step_does_not_stop_others(ops_app, monkeypatch):
    svc = ops_app.ctx.service("retention")
    monkeypatch.setattr(svc, "_step_events", lambda dry: (_ for _ in ()).throw(RuntimeError("boom")))
    rep = svc.run(None, only=["events", "tokens"])
    assert rep["steps"]["events"]["error"].startswith("RuntimeError") and "deleted" in rep["steps"]["tokens"]
    runs = svc.runs(1)
    assert runs[0]["error"] == "one or more steps failed"


# ----------------------------------------------------------------------------- scheduling and authorisation
def test_tick_runs_on_schedule_only(ops_app):
    from datetime import UTC, datetime, timedelta

    c = ops_app
    svc = c.ctx.service("retention")
    first = svc.tick()
    assert first.get("run")
    assert svc.tick() == {}  # inside the interval
    assert svc.tick(datetime.now(UTC) + timedelta(hours=25)).get("run")
    assert put(c, "retention.enabled", 0).status_code == 200
    assert svc.tick(datetime.now(UTC) + timedelta(hours=60)) == {}
    # the module loop is leader-only
    from olcf.ops.module import OpsModule

    mod = next(m for m in c.ctx.modules if isinstance(m, OpsModule))
    assert put(c, "retention.enabled", 1).status_code == 200
    c.ctx.leader._leader = False
    assert mod.tick() == {}


def test_authorization(ops_app):
    c = ops_app
    pw = "Correct-Horse-9-battery"
    expect = {  # role -> (status read, dry run, real run, audit verify)
        "Viewer": (False, False, False, False),
        "Operator": (False, False, False, False),
        "Contributor": (False, False, False, False),
        "RuntimeAdmin": (True, True, False, False),
        "SecurityAdmin": (False, False, False, True),
    }
    for role, (read, dry, real, aud) in expect.items():
        assert c.post("/v1/identity/users", json={"name": f"r-{role.lower()}", "password": pw, "roles": [{"role": role}]}, headers=c.admin).status_code == 201
        h = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': f'r-{role.lower()}', 'password': pw}).json()['access_token']}"}
        assert (c.get("/v1/admin/retention", headers=h).status_code == 200) == read, role
        assert (c.post("/v1/admin/retention/run", json={"dry_run": True}, headers=h).status_code == 200) == dry, role
        assert (c.post("/v1/admin/retention/run", json={"dry_run": False}, headers=h).status_code == 200) == real, role
        assert (c.get("/v1/admin/retention/audit", headers=h).status_code == 200) == aud, role
    st = c.get("/v1/admin/retention", headers=c.admin).json()
    assert st["enabled"] and st["audit"]["ok"] and st["settings"]["audit_days"] >= 30
    for m, p in (("get", "/v1/admin/retention"), ("post", "/v1/admin/retention/run"), ("get", "/v1/admin/retention/runs"), ("get", "/v1/admin/retention/audit")):
        assert getattr(c, m)(p).status_code == 401
