# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Update preflight (real migration dry-run), apply, drain, hand-off, the 'no unsigned path' guarantees, reconcile and rollback."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

from olcf.ops import manifest as M
from olcf.ops import upgrade as U
from tests.ops.feedserver import FeedServer
from tests.ops.updateutil import feed_files, make_bundle, make_release


def _import(c, rel, tmp_path, name="b.tar.gz"):
    r = c.post("/v1/admin/updates/import", files={"file": (name, make_bundle(tmp_path, rel, name=name).read_bytes())}, headers=c.admin)
    assert r.status_code == 201, r.text
    return r.json()


def _preflight(c, uid):
    r = c.post(f"/v1/admin/updates/{uid}/preflight?wait=true", headers=c.admin)
    assert r.status_code == 200, r.text
    op = r.json()["operation"]
    assert op["state"] == "succeeded", op
    return op["result"]


def _apply(c, uid, **kw):
    r = c.post(f"/v1/admin/updates/{uid}/apply?wait=true", json={"confirm": "APPLY", **kw}, headers=c.admin)
    return r


def checks(res):
    return {c["name"]: c for c in res["checks"]}


class FakeRegistry:
    def __init__(self) -> None:
        self.workers = [{"id": "wrk_1", "name": "w1", "scheduling": "active", "agent_version": "0.1.0"}, {"id": "wrk_2", "name": "w2", "scheduling": "active", "agent_version": "0.0.5"},
                        {"id": "wrk_3", "name": "w3", "scheduling": "cordoned", "agent_version": "0.1.0"}]
        self.queues = [{"name": "default", "paused": False}, {"name": "gpu", "paused": True}]
        self.queue_calls: list[tuple[str, bool]] = []

    def list_workers(self, **_: object):  # noqa: ANN201
        return self.workers

    def list_queues(self):  # noqa: ANN201
        return self.queues

    def update_queue(self, name, changes, *, fields_set):  # noqa: ANN001, ANN201
        for q in self.queues:
            if q["name"] == name:
                q["paused"] = changes["paused"]
        self.queue_calls.append((name, changes["paused"]))


class FakeAdmin:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def set_scheduling(self, principal, ref, new, reason=""):  # noqa: ANN001, ANN201
        self.calls.append((ref, new))


class FakeJobs:
    def __init__(self, running: int) -> None:
        self.running = running

    def counts(self):  # noqa: ANN201
        r = self.running
        if self.running > 0:
            self.running -= 1  # jobs finish while we wait
        return {"queued": 3, "running": r}


class FakeCompute:
    def __init__(self, running: int = 2) -> None:
        self.registry, self.admin, self.jobs = FakeRegistry(), FakeAdmin(), FakeJobs(running)


# ----------------------------------------------------------------------------- preflight
def test_preflight_runs_real_migration_dry_run(upd, tmp_path):
    c = upd
    c.ctx.secrets.create("data", "x")
    rel = make_release(tmp_path, c.signer, "0.2.0", migration="extra", migrations={"ops": 2}, compat={"python": ">=3.11", "databases": ["sqlite", "postgresql"]})
    u = _import(c, rel, tmp_path)
    res = _preflight(c, u["id"])
    ck = checks(res)
    assert res["ok"], res
    assert "applied to a copy" in ck["migration-dry-run"]["detail"] and "'ops': 2" in ck["migration-dry-run"]["detail"]
    assert {"signature", "version", "python", "database", "artifacts", "disk", "workers", "migration-dry-run", "maintenance-window", "rollback"} <= set(ck)
    # the LIVE database was not touched
    with c.ctx.db.connect() as conn:
        assert conn.execute(text("SELECT MAX(version) FROM schema_migrations WHERE stream='ops'")).scalar() == 1
        cols = [r[1] for r in conn.exec_driver_sql("PRAGMA table_info(retention_runs)")]
    assert "note" not in cols
    assert c.get(f"/v1/admin/updates/{u['id']}", headers=c.admin).json()["preflight"]["ok"]


def test_preflight_fails_on_broken_migration_and_apply_refuses(upd, tmp_path):
    c = upd
    rel = make_release(tmp_path, c.signer, "0.2.0", migration="broken")
    u = _import(c, rel, tmp_path)
    res = _preflight(c, u["id"])
    assert not res["ok"] and "exploded on purpose" in checks(res)["migration-dry-run"]["detail"]
    fake = FakeCompute()
    c.ctx.register_service("compute", fake)
    r = _apply(c, u["id"])
    assert r.status_code == 202
    op = r.json()["operation"]
    assert op["state"] == "failed" and "preflight failed" in op["message"]
    assert not c.ctx.service("updates").marker_path.exists()
    assert fake.admin.calls == [] and fake.registry.queue_calls == []  # compute was never touched
    assert not [b for b in c.ctx.service("backup").list() if b["kind"] == "pre-update"]
    assert c.get(f"/v1/admin/updates/{u['id']}", headers=c.admin).json()["state"] == "failed"


def test_preflight_catches_lying_artifacts_and_compat(upd, tmp_path):
    c = upd
    # the wheel's code says 0.2.5 although the signed manifest says 0.2.0
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0", sequence=1, code_version="0.2.5"), tmp_path, "a.tar.gz")
    res = _preflight(c, u["id"])
    assert not res["ok"] and "0.2.5" in checks(res)["migration-dry-run"]["detail"]
    # python / database / min_version incompatibilities
    u2 = _import(c, make_release(tmp_path, c.signer, "0.3.0", sequence=2, compat={"python": "<3.0", "databases": ["mysql"]}, min_version="0.2.0"), tmp_path, "b.tar.gz")
    ck = checks(_preflight(c, u2["id"]))
    assert ck["python"]["status"] == "fail" and ck["database"]["status"] == "fail" and ck["version"]["status"] == "fail" and "0.2.0" in ck["version"]["detail"]
    # migrations that would downgrade the schema
    u3 = _import(c, make_release(tmp_path, c.signer, "0.4.0", sequence=3, migrations={"core": 0}), tmp_path, "c.tar.gz")
    assert checks(_preflight(c, u3["id"]))["migration-dry-run"]["status"] == "fail"


def test_preflight_worker_versions_and_jobs(upd, tmp_path):
    c = upd
    c.ctx.register_service("compute", FakeCompute(running=2))
    rel = make_release(tmp_path, c.signer, "0.2.0", compat={"workers": {"min_version": "0.1.0", "required": False}})
    ck = checks(_preflight(c, _import(c, rel, tmp_path)["id"]))
    assert ck["workers"]["status"] == "warn" and "w2" in ck["workers"]["detail"] and ck["workers"]["versions"]["w1"] == "0.1.0"
    assert ck["active-jobs"]["status"] == "warn"
    rel2 = make_release(tmp_path, c.signer, "0.3.0", sequence=2, compat={"workers": {"min_version": "0.1.0", "required": True}})
    assert checks(_preflight(c, _import(c, rel2, tmp_path, "b.tar.gz")["id"]))["workers"]["status"] == "fail"


def test_disk_space_check(upd, tmp_path, monkeypatch):
    c = upd
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0"), tmp_path)
    real = shutil.disk_usage

    class DU:
        free = 1024 * 1024
        total = used = 0

    monkeypatch.setattr(shutil, "disk_usage", lambda p: DU if str(p).startswith(str(c.ctx.settings.data_dir)) else real(p))
    assert checks(_preflight(c, u["id"]))["disk"]["status"] == "fail"


# ----------------------------------------------------------------------------- windows + drain + apply
def test_maintenance_window_enforced_and_override_audited(upd, tmp_path):
    c = upd
    now = datetime.now(UTC)
    far = (now + timedelta(hours=6)).hour
    _cfg = c.put("/v1/admin/updates/config", json={"maintenance_windows": [{"cron": f"0 {far} * * *", "minutes": 30}], "fetch_previous": False}, headers=c.admin)
    assert _cfg.status_code == 200
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0"), tmp_path)
    r = c.post(f"/v1/admin/updates/{u['id']}/apply", json={"confirm": "APPLY"}, headers=c.admin)
    assert r.status_code == 409 and "window" in r.text
    assert c.post(f"/v1/admin/updates/{u['id']}/apply", json={"confirm": "yes"}, headers=c.admin).status_code == 400
    svc = c.ctx.service("updates")
    assert svc.in_window(now.replace(hour=far, minute=10))[0] is True
    assert svc.in_window(now.replace(hour=far, minute=45))[0] is False
    r = _apply(c, u["id"], override_window=True)
    assert r.json()["operation"]["state"] == "succeeded", r.text
    assert c.ctx.audit.query(limit=50, action="admin.update_window_override")
    assert c.put("/v1/admin/updates/config", json={"maintenance_windows": [{"cron": "bad", "minutes": 5}]}, headers=c.admin).status_code == 400


def test_apply_drains_backs_up_and_hands_off(upd, tmp_path):
    c = upd
    c.put("/v1/admin/updates/config", json={"fetch_previous": False, "restart_command": None}, headers=c.admin)
    fake = FakeCompute(running=2)
    c.ctx.register_service("compute", fake)
    c.ctx.secrets.create("pre-update-data", "v")
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0", migration="extra", migrations={"ops": 2}), tmp_path)
    r = _apply(c, u["id"])
    assert r.status_code == 202, r.text
    op = r.json()["operation"]
    assert op["state"] == "succeeded", op
    res = op["result"]
    assert res["handoff"] is True and res["from"] == "0.1.0" and res["to"] == "0.2.0"
    # backup first
    bk = c.ctx.service("backup").get(res["backup_id"])
    assert bk["kind"] == "pre-update" and bk["state"] == "succeeded"
    # compute drained: active workers draining (cordoned left alone), only non-paused queue paused
    assert sorted(fake.admin.calls) == [("wrk_1", "draining"), ("wrk_2", "draining")]
    assert fake.registry.queue_calls == [("default", True)]
    assert c.ctx.service("updates").maintenance_state()["active"] is True
    # marker is complete and the update is in hand-off
    marker = json.loads(c.ctx.service("updates").marker_path.read_text())
    assert marker["state"] == "handoff" and marker["version"] == "0.2.0" and marker["backup_id"] == bk["id"] and marker["trusted_keys"]
    assert c.get(f"/v1/admin/updates/{u['id']}", headers=c.admin).json()["state"] == "handoff"
    # a second apply while in hand-off is refused
    assert c.post(f"/v1/admin/updates/{u['id']}/apply", json={"confirm": "APPLY"}, headers=c.admin).status_code == 409
    # --- install via the hand-off CLI into an isolated target (no network)
    target = tmp_path / "site"
    out = U.apply(c.ctx.settings.data_dir, target=str(target), extra_pip=["--no-deps", "--no-index"])
    assert out["ok"], out
    assert '__version__ = "0.2.0"' in (target / "olcf" / "version.py").read_text()
    # --- "restart": the new version is running -> reconcile completes the update and resumes compute
    import olcf.ops.updates as upd_mod

    old = upd_mod.installed_version
    upd_mod.installed_version = lambda: "0.2.0"  # type: ignore[assignment]
    try:
        rec = c.ctx.service("updates").reconcile()
    finally:
        upd_mod.installed_version = old
    assert rec["applied"] == "0.2.0" and rec["state"] == "applied"
    assert not c.ctx.service("updates").marker_path.exists()
    assert c.ctx.service("updates").maintenance_state()["active"] is False
    assert ("wrk_1", "active") in fake.admin.calls and ("wrk_2", "active") in fake.admin.calls and ("wrk_3", "active") not in fake.admin.calls
    assert ("default", False) in fake.registry.queue_calls
    assert fake.registry.queues[1]["paused"] is True, "a queue that was already paused stays paused"
    assert c.get(f"/v1/admin/updates/{u['id']}", headers=c.admin).json()["state"] == "applied"


def test_drain_timeout_resumes_compute(upd, tmp_path):
    c = upd
    c.put("/v1/admin/updates/config", json={"drain_timeout_seconds": 1}, headers=c.admin)
    fake = FakeCompute(running=10_000)
    c.ctx.register_service("compute", fake)
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0"), tmp_path)
    op = _apply(c, u["id"]).json()["operation"]
    assert op["state"] == "failed" and "still running" in op["message"]
    assert c.ctx.service("updates").maintenance_state()["active"] is False
    assert ("default", False) in fake.registry.queue_calls and ("wrk_1", "active") in fake.admin.calls
    assert not c.ctx.service("updates").marker_path.exists()
    # cancelling a failed update is harmless and the state allows retry
    assert c.post(f"/v1/admin/updates/{u['id']}/cancel", headers=c.admin).status_code == 200


def test_manual_maintenance_toggle(upd):
    c = upd
    fake = FakeCompute(running=0)
    c.ctx.register_service("compute", fake)
    r = c.post("/v1/admin/updates/maintenance", json={"on": True}, headers=c.admin)
    assert r.status_code == 200 and r.json()["maintenance"]["active"] is True
    r = c.post("/v1/admin/updates/maintenance", json={"on": False}, headers=c.admin)
    assert r.json()["maintenance"]["active"] is False and ("default", False) in fake.registry.queue_calls


def test_runtime_bundle_artifacts_go_through_runtimes_service(upd, tmp_path):
    c = upd
    saved = []

    class FakeRuntimes:
        def save_bundle(self, principal, filename, stream, runtime_id):  # noqa: ANN001, ANN201
            data = stream.read()
            saved.append((filename, runtime_id, len(data)))
            import hashlib

            return {"id": "bun_1", "sha256": hashlib.sha256(data).hexdigest()}

    c.ctx.register_service("runtimes", FakeRuntimes())
    rel = make_release(tmp_path, c.signer, "0.2.0", extra_artifacts=[("runtime-bundle", "rt-2.0.tar.zst", b"RUNTIME-BYTES" * 100, "olcf-rt-2.0.0")])
    u = _import(c, rel, tmp_path)
    op = _apply(c, u["id"]).json()["operation"]
    assert op["state"] == "succeeded", op
    assert saved == [("rt-2.0.tar.zst", "olcf-rt-2.0.0", 1300)]
    assert op["result"]["runtime_bundles"][0]["bundle_id"] == "bun_1"


# ----------------------------------------------------------------------------- no unsigned path
def test_apply_has_no_way_to_skip_verification(upd):
    from olcf.ops.updates_api import ApplyBody, ConfigBody

    assert set(ApplyBody.model_fields) == {"confirm", "override_window", "execute"}
    assert not [f for f in ConfigBody.model_fields if "verify" in f or "sign" in f or "trust_all" in f or "insecure" in f.replace("allow_insecure_http", "")]
    paths = upd.get("/v1/openapi.json").json()["paths"]
    assert not [p for p in paths if "/admin/updates" in p and any(w in p for w in ("force", "unsigned", "skip"))]


def _prepared(c, tmp_path, **kw):
    c.put("/v1/admin/updates/config", json={"fetch_previous": False}, headers=c.admin)
    rel = make_release(tmp_path, c.signer, "0.2.0", **kw)
    u = _import(c, rel, tmp_path)
    op = _apply(c, u["id"]).json()["operation"]
    assert op["state"] == "succeeded", op
    return u, rel, json.loads(c.ctx.service("updates").marker_path.read_text())


def test_handoff_refuses_tampered_staged_artifacts(upd, tmp_path):
    c = upd
    u, rel, marker = _prepared(c, tmp_path)
    data_dir = c.ctx.settings.data_dir
    wheel, mf = Path(marker["wheel"]), Path(marker["manifest"])
    good_wheel, good_mf = wheel.read_bytes(), mf.read_bytes()
    tgt = str(tmp_path / "t")
    pip = ["--no-deps", "--no-index"]
    # 1. wheel swapped after verification
    wheel.write_bytes(good_wheel[:-20] + b"EVIL" * 5)
    with pytest.raises(U.UpgradeError, match="SHA-256"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    wheel.write_bytes(good_wheel)
    # 2. signatures stripped
    env = json.loads(good_mf)
    mf.write_text(json.dumps({**env, "signatures": []}))
    with pytest.raises(U.UpgradeError, match="unsigned"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    # 3. payload changed (new version/artifact hash) but old signature kept
    p = json.loads(__import__("base64").b64decode(env["payload"]))
    p["artifacts"][0]["sha256"] = "0" * 64
    mf.write_text(json.dumps({**env, "payload": __import__("base64").b64encode(M.canonical(p)).decode()}))
    with pytest.raises(U.UpgradeError, match="signature"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    # 4. signed by somebody else
    other = M.generate_keypair()[0]
    mf.write_text(json.dumps(M.sign_manifest(rel.payload, other)))
    with pytest.raises(U.UpgradeError, match="trust"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    # 5. key revoked in the trust snapshot
    mf.write_bytes(good_mf)
    mk = json.loads((data_dir / "updates" / "handoff.json").read_text())
    mk["trusted_keys"][0]["revoked"] = True
    (data_dir / "updates" / "handoff.json").write_text(json.dumps(mk))
    with pytest.raises(U.UpgradeError, match="revoked"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    # 6. marker redirected to a wheel outside the signed set
    mk["trusted_keys"][0]["revoked"] = False
    evil = tmp_path / "evil-0.2.0-py3-none-any.whl"
    evil.write_bytes(good_wheel)
    mk["wheel"] = str(evil)
    (data_dir / "updates" / "handoff.json").write_text(json.dumps(mk))
    with pytest.raises(U.UpgradeError, match="not the signed artifact"):
        U.apply(data_dir, target=tgt, extra_pip=pip)
    assert not Path(tgt).exists(), "nothing was installed by any refused attempt"
    # 7. and the untouched original still installs
    mk["wheel"] = marker["wheel"]
    (data_dir / "updates" / "handoff.json").write_text(json.dumps(mk))
    assert U.apply(data_dir, target=tgt, extra_pip=pip)["ok"]


def test_handoff_refuses_expired_manifest_and_wrong_wheel_metadata(upd, tmp_path):
    c = upd
    soon = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    u, rel, marker = _prepared(c, tmp_path, expires_at=soon)
    mk_path = c.ctx.service("updates").marker_path
    # the staged manifest expires before anyone runs the hand-off (e.g. staged and forgotten)
    mf = Path(marker["manifest"])
    stale = dict(rel.payload, expires_at=(datetime.now(UTC) - timedelta(days=1)).isoformat())
    mf.write_text(json.dumps(M.sign_manifest(stale, c.signer.priv)))
    with pytest.raises(U.UpgradeError, match="expired"):
        U.apply(c.ctx.settings.data_dir, target=str(tmp_path / "t"), extra_pip=["--no-deps", "--no-index"])
    assert mk_path.exists()


def test_apply_reverifies_stored_record(upd, tmp_path):
    """Tampering with the database record (envelope swapped for an unsigned/forged one) cannot get an update applied."""
    c = upd
    u = _import(c, make_release(tmp_path, c.signer, "0.2.0"), tmp_path)
    from sqlalchemy import select, update

    from olcf.ops import schema as O

    with c.ctx.db.connect() as conn:
        row = conn.execute(select(O.update_records.c.manifest).where(O.update_records.c.id == u["id"])).scalar_one()
    forged = {"envelope": {**row["envelope"], "signatures": []}, "payload": row["payload"]}
    with c.ctx.db.begin() as conn:
        conn.execute(update(O.update_records).where(O.update_records.c.id == u["id"]).values(manifest=forged))
    r = c.post(f"/v1/admin/updates/{u['id']}/apply", json={"confirm": "APPLY"}, headers=c.admin)
    assert r.status_code == 400 and "unsigned" in r.text
    assert c.post(f"/v1/admin/updates/{u['id']}/preflight?wait=true", headers=c.admin).json()["operation"]["state"] == "failed"
    assert not c.ctx.service("updates").marker_path.exists()


def test_stage_downloads_and_verifies_from_feed(upd, tmp_path):
    c = upd
    rel = make_release(tmp_path, c.signer, "0.2.0")
    srv = FeedServer(feed_files(rel)).start()
    try:
        c.put("/v1/admin/updates/config", json={"feed_url": srv.url, "allow_insecure_http": True, "fetch_previous": False}, headers=c.admin)
        uid = c.post("/v1/admin/updates/check", json={}, headers=c.admin).json()["update"]["id"]
        assert c.get(f"/v1/admin/updates/{uid}", headers=c.admin).json()["staged"] is False
        op = c.post(f"/v1/admin/updates/{uid}/stage?wait=true", headers=c.admin).json()["operation"]
        assert op["state"] == "succeeded" and op["result"]["staged"] is True
        assert (c.ctx.settings.path("updates") / "0.2.0-stable" / rel.wheel.name).is_file()
        # the server is compromised and serves a different wheel than the signed one
        shutil.rmtree(c.ctx.settings.path("updates") / "0.2.0-stable")
        srv.files[f"/stable/{rel.wheel.name}"] = rel.wheel.read_bytes()[:-5] + b"EVIL!"
        op = c.post(f"/v1/admin/updates/{uid}/stage?wait=true", headers=c.admin).json()["operation"]
        assert op["state"] == "failed" and "signed hash" in op["message"]
        assert not list((c.ctx.settings.path("updates") / "0.2.0-stable").glob("*.whl"))
    finally:
        srv.stop()


# ----------------------------------------------------------------------------- cancel + rollback
def test_cancel_before_restart(upd, tmp_path):
    c = upd
    fake = FakeCompute(running=0)
    c.ctx.register_service("compute", fake)
    u, rel, marker = _prepared(c, tmp_path)
    assert c.ctx.service("updates").maintenance_state()["active"]
    r = c.post(f"/v1/admin/updates/{u['id']}/cancel", headers=c.admin)
    assert r.status_code == 200 and r.json()["state"] == "rolled_back"
    assert not c.ctx.service("updates").marker_path.exists() and not c.ctx.service("updates").maintenance_state()["active"]
    assert ("default", False) in fake.registry.queue_calls


def test_rollback_restores_database_and_previous_version(upd, tmp_path):
    from tests.ops.wheelutil import build_olcf_wheel

    c = upd
    ctx = c.ctx
    ctx.secrets.create("before-update", "keep-me")
    u, rel, marker = _prepared(c, tmp_path, migration="extra", migrations={"ops": 2})
    data_dir = ctx.settings.data_dir
    # new release "runs": schema migrated, new data written
    import olcf.ops.updates as upd_mod

    old = upd_mod.installed_version
    upd_mod.installed_version = lambda: "0.2.0"  # type: ignore[assignment]
    try:
        ctx.service("updates").reconcile()
        with ctx.db.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE retention_runs ADD COLUMN note TEXT")  # what the new release's migration did
            conn.exec_driver_sql("INSERT INTO schema_migrations (stream, version, description, applied_at) VALUES ('ops', 2, 'new', '2026-01-01T00:00:00Z')")
        ctx.secrets.create("after-update", "lose-me")
        out = c.post(f"/v1/admin/updates/{u['id']}/rollback", headers=c.admin)
        assert out.status_code == 200, out.text
        assert (data_dir / "updates" / "rollback.json").is_file()
        assert c.get(f"/v1/admin/updates/{u['id']}", headers=c.admin).json()["state"] == "rollback_pending"
    finally:
        upd_mod.installed_version = old
    ctx.db.dispose()  # "stop the platform"
    prev = build_olcf_wheel(tmp_path / "prev", "0.1.0")
    target = tmp_path / "prev-site"
    res = U.rollback(data_dir, previous_wheel=prev, target=str(target), extra_pip=["--no-deps", "--no-index"])
    assert res["ok"] and res["restore_returncode"] == 0, res
    assert '__version__ = "0.1.0"' in (target / "olcf" / "version.py").read_text()
    # the database is exactly the pre-update state: fresh schema (no 'note' column), no post-update secret, pre-update secret present
    import sqlite3

    con = sqlite3.connect(data_dir / "olcf.db")
    try:
        names = {r[0] for r in con.execute("SELECT name FROM secrets")}
        cols = [r[1] for r in con.execute("PRAGMA table_info(retention_runs)")]
        ops_ver = con.execute("SELECT MAX(version) FROM schema_migrations WHERE stream='ops'").fetchone()[0]
    finally:
        con.close()
    assert "before-update" in names and "after-update" not in names
    assert "note" not in cols and ops_ver == 1
    assert not (data_dir / "updates" / "rollback.json").exists()


def test_rollback_refuses_when_backup_missing(upd, tmp_path):
    c = upd
    u, rel, marker = _prepared(c, tmp_path)
    data_dir = c.ctx.settings.data_dir
    (data_dir / "updates").mkdir(exist_ok=True)
    (data_dir / "updates" / "rollback.json").write_text(json.dumps({"state": "rollback", "backup_file": "../../etc/passwd", "to_version": "0.1.0"}))
    with pytest.raises(U.UpgradeError, match="not in"):
        U.rollback(data_dir, skip_code=True)
    (data_dir / "updates" / "rollback.json").write_text(json.dumps({"state": "rollback", "backup_file": "olcf-backup-nope.olcfbak", "to_version": "0.1.0"}))
    with pytest.raises(U.UpgradeError, match="not in"):
        U.rollback(data_dir, skip_code=True)


def test_cli_status_and_refusals(upd, tmp_path, capsys):
    dd = tmp_path / "empty"
    dd.mkdir()
    assert U.main(["apply", "--data-dir", str(dd)]) == 2
    assert "REFUSED" in capsys.readouterr().err
    assert U.main(["status", "--data-dir", str(dd)]) == 0
    assert json.loads(capsys.readouterr().out) == {}
