# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Backup API: create, targets, schedule/retention, validate, restore-test, restore, import/download, authz."""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text

from olcf.db import schema as S
from tests.ops.conftest import CANARY, PASSPHRASE


def _backup(c, **body):
    r = c.post("/v1/admin/backup?wait=true", json=body, headers=c.admin)
    assert r.status_code == 202, r.text
    return r.json()


def test_backup_lifecycle_and_restore_test(ops_app):
    c = ops_app
    c.ctx.secrets.create("canary", CANARY)
    out = _backup(c)
    bk = out["backup"]
    assert bk["state"] == "succeeded", bk
    assert bk["encryption"] == "master" and bk["rows"] > 0 and bk["last_validation"]["ok"]
    r = c.post(f"/v1/admin/backup/{bk['id']}/restore-test?wait=true", json={}, headers=c.admin)
    assert r.status_code == 200, r.text
    res = r.json()["operation"]
    assert res["state"] == "succeeded", res
    assert res["result"]["ok"], res["result"]
    names = {x["name"] for x in res["result"]["checks"]}
    assert {"validate", "restore", "row-counts", "integrity", "audit-chain", "secrets-decrypt", "admin-binding"} <= names
    got = c.get(f"/v1/admin/backup/{bk['id']}", headers=c.admin).json()
    assert got["last_restore_test"]["ok"] is True
    assert c.get("/v1/admin/backup", headers=c.admin).json()["items"][0]["id"] == bk["id"]
    # download is the encrypted file
    d = c.get(f"/v1/admin/backup/{bk['id']}/download", headers=c.admin)
    assert d.status_code == 200 and CANARY.encode() not in d.content and d.content.startswith(b"OLCFBAK1")
    # audit trail
    actions = {a["action"] for a in c.ctx.audit.query(limit=50)}
    assert {"admin.backup_created", "admin.backup_restore_tested", "admin.backup_downloaded"} <= actions


def _login_as(c, name, role):
    pw = "Correct-Horse-9-battery"
    r = c.post("/v1/identity/users", json={"name": name, "password": pw, "roles": [{"role": role}]}, headers=c.admin)
    assert r.status_code == 201, r.text
    tok = c.post("/v1/identity/login", json={"username": name, "password": pw})
    assert tok.status_code == 200, tok.text
    return {"Authorization": f"Bearer {tok.json()['access_token']}"}


@pytest.mark.parametrize("role", ["Viewer", "SecurityAdmin", "RuntimeAdmin", "Operator", "Contributor"])
def test_requires_admin_backup_permission(ops_app, role):
    c = ops_app
    assert c.post("/v1/admin/backup", json={}, headers={}).status_code == 401
    h = _login_as(c, f"user-{role.lower()}", role)
    bk = _backup(c)["backup"]
    for method, path in (("get", "/v1/admin/backup"), ("post", "/v1/admin/backup"), ("get", "/v1/admin/backup/config"), ("put", "/v1/admin/backup/config"),
                         ("get", "/v1/admin/backup/targets"), ("post", "/v1/admin/backup/targets"), ("get", f"/v1/admin/backup/{bk['id']}"),
                         ("get", f"/v1/admin/backup/{bk['id']}/download"), ("post", f"/v1/admin/backup/{bk['id']}/restore"),
                         ("post", f"/v1/admin/backup/{bk['id']}/validate"), ("delete", f"/v1/admin/backup/{bk['id']}"), ("post", "/v1/admin/backup/retention")):
        body = {"name": "x", "type": "local"} if path.endswith("/targets") else {"confirm": "RESTORE"} if path.endswith("/restore") else {}
        kw = {"json": body} if method in ("post", "put") else {}
        r = getattr(c, method)(path, headers=h, **kw)
        assert r.status_code == 403, (role, method, path, r.status_code)


def test_include_keys_requires_passphrase_and_roundtrips_keys(ops_app, tmp_path):
    c = ops_app
    r = c.post("/v1/admin/backup", json={"include_keys": True}, headers=c.admin)
    assert r.status_code == 400
    bk = _backup(c, include_keys=True, passphrase=PASSPHRASE)["backup"]
    assert bk["state"] == "succeeded" and bk["encryption"] == "passphrase" and bk["includes"]["keys"] is True
    from olcf.ops import backup_core as core
    from olcf.ops.crypto import KeySpec

    path = c.ctx.settings.path("backups", bk["filename"])
    rep = core.validate_backup(path, KeySpec(PASSPHRASE))
    assert rep.ok
    assert {"keys/secrets/master.key", "keys/pki/ca.key", "keys/secrets/jwt-signing.key"} <= set(rep.manifest["files"])
    # the master key bytes are not readable without the passphrase
    master = c.ctx.settings.path("secrets", "master.key").read_bytes().strip()
    assert master not in path.read_bytes()
    # wrong passphrase on restore-test fails clearly
    r = c.post(f"/v1/admin/backup/{bk['id']}/restore-test?wait=true", json={"passphrase": "totally wrong pass 1234"}, headers=c.admin)
    assert r.json()["operation"]["result"]["ok"] is False


def test_targets_local_and_retention(ops_app, tmp_path):
    c = ops_app
    tdir = tmp_path / "offhost"
    r = c.post("/v1/admin/backup/targets", json={"name": "nas", "type": "local", "config": {"path": str(tdir)}}, headers=c.admin)
    assert r.status_code == 201, r.text
    t = c.post("/v1/admin/backup/targets/nas/test", headers=c.admin).json()
    assert t["ok"], t
    assert [s["name"] for s in t["steps"]] == ["write", "read-back", "delete"]
    r = c.put("/v1/admin/backup/config", json={"targets": ["nas"], "retention": {"keep_last": 2, "keep_days": 0}}, headers=c.admin)
    assert r.status_code == 200, r.text
    ids = [_backup(c)["backup"] for _ in range(1)]
    assert ids[0]["targets"][0]["ok"] and (tdir / ids[0]["filename"]).is_file()
    # retention: fabricate old files and prune
    svc = c.ctx.service("backup")
    old = [f"olcf-backup-2026010{i}T000000Z-abc{i}.olcfbak" for i in range(1, 6)]
    for n in old:
        (tdir / n).write_bytes(b"x")
    rep = c.post("/v1/admin/backup/retention?dry_run=true", headers=c.admin).json()
    assert sorted(rep["targets"]["nas"]) == sorted(old[:4]) or len(rep["targets"]["nas"]) >= 3
    assert all((tdir / n).exists() for n in old), "dry run must not delete"
    c.post("/v1/admin/backup/retention?dry_run=false", headers=c.admin)
    left = {p.name for p in tdir.glob("*.olcfbak")}
    assert ids[0]["filename"] in left and len(left) == 2
    # target in use cannot be deleted; a bad path is refused
    assert c.delete("/v1/admin/backup/targets/nas", headers=c.admin).status_code == 409
    assert c.post("/v1/admin/backup/targets", json={"name": "bad", "type": "local", "config": {"path": "relative/path"}}, headers=c.admin).status_code == 400
    assert c.post("/v1/admin/backup/targets", json={"name": "bad2", "type": "local", "config": {"path": str(c.ctx.settings.path("secrets", "x"))}}, headers=c.admin).status_code == 400
    _ = svc


def test_prune_names_semantics():
    from olcf.ops.backup import BackupService

    now = datetime(2026, 3, 1, tzinfo=UTC)
    names = [f"olcf-backup-{(now - timedelta(days=d)).strftime('%Y%m%dT%H%M%SZ')}-x.olcfbak" for d in range(0, 10)]
    gone = BackupService._prune_names(names, keep_last=3, keep_days=0, now=now)
    assert len(gone) == 7 and names[0] not in gone
    gone = BackupService._prune_names(names, keep_last=0, keep_days=4, now=now)
    assert names[0] not in gone and names[3] not in gone and names[5] in gone
    assert BackupService._prune_names(["junk.olcfbak"], 1, 1, now) == []


def test_config_validation_and_passphrase_not_leaked(ops_app):
    c = ops_app
    bad = c.put("/v1/admin/backup/config", json={"schedule": {"enabled": True, "cron": "not a cron"}}, headers=c.admin)
    assert bad.status_code == 400
    assert c.put("/v1/admin/backup/config", json={"retention": {"keep_last": 0, "keep_days": 0}}, headers=c.admin).status_code == 400
    assert c.put("/v1/admin/backup/config", json={"targets": ["nope"]}, headers=c.admin).status_code == 400
    ok = c.put("/v1/admin/backup/config", json={"passphrase": PASSPHRASE, "schedule": {"enabled": True, "cron": "0 3 * * *"}}, headers=c.admin)
    assert ok.status_code == 200 and ok.json()["passphrase_configured"] is True
    assert PASSPHRASE not in ok.text and PASSPHRASE not in c.get("/v1/admin/backup/config", headers=c.admin).text
    assert c.put("/v1/admin/backup/config", json={"passphrase": "short"}, headers=c.admin).status_code == 400
    # stored passphrase now drives unattended backups
    bk = _backup(c)["backup"]
    assert bk["encryption"] == "passphrase"


def test_scheduler_tick_fires_once_per_slot(ops_app):
    c = ops_app
    svc = c.ctx.service("backup")
    c.put("/v1/admin/backup/config", json={"schedule": {"enabled": True, "cron": "0 * * * *"}}, headers=c.admin)
    t0 = datetime(2026, 5, 1, 10, 30, tzinfo=UTC)
    assert svc.tick(t0) == {}  # anchors, no retroactive firing
    assert svc.tick(t0 + timedelta(minutes=10)) == {}
    did = svc.tick(t0 + timedelta(minutes=31))
    assert "backup" in did
    assert svc.tick(t0 + timedelta(minutes=32)) == {}  # same slot never fires twice
    items = [b for b in svc.list() if b["kind"] == "scheduled"]
    assert len(items) == 1 and items[0]["state"] == "succeeded"


def test_restore_into_live_db_roundtrip(ops_app):
    c = ops_app
    svc = c.ctx.service("backup")
    s1 = c.ctx.secrets.create("before", "value-before")
    bk = _backup(c)["backup"]
    c.ctx.secrets.create("after-backup", "value-after")
    with c.ctx.db.connect() as conn:
        n_before = conn.execute(text("SELECT COUNT(*) FROM secrets")).scalar()
    # confirmation required
    assert c.post(f"/v1/admin/backup/{bk['id']}/restore", json={"confirm": "yes"}, headers=c.admin).status_code == 400
    pre = c.get(f"/v1/admin/backup/{bk['id']}/prerequisites", headers=c.admin).json()
    assert any(p["name"] == "effect" for p in pre["prerequisites"])
    r = c.post(f"/v1/admin/backup/{bk['id']}/restore?wait=true", json={"confirm": "RESTORE"}, headers=c.admin)
    assert r.status_code == 202, r.text
    op = c.get(f"/v1/operations/{r.json()['operation_id']}", headers=c.admin).json()
    assert op["state"] == "succeeded", op
    with c.ctx.db.connect() as conn:
        names = {x[0] for x in conn.execute(select(S.secrets.c.name))}
    assert "before" in names and "after-backup" not in names
    assert op["result"]["pre_restore_backup"]
    # the pre-restore backup file is still listed and restorable (undo)
    kinds = {b["kind"] for b in svc.list()}
    assert "pre-restore" in kinds
    assert svc.get(op["result"]["pre_restore_backup"])["state"] == "succeeded"
    _ = (s1, n_before)


def test_import_and_download_roundtrip(ops_app):
    c = ops_app
    bk = _backup(c)["backup"]
    data = c.get(f"/v1/admin/backup/{bk['id']}/download", headers=c.admin).content
    r = c.post("/v1/admin/backup/import", files={"file": ("../../evil name.olcfbak", io.BytesIO(data))}, headers=c.admin)
    assert r.status_code == 201, r.text
    imp = r.json()
    assert "/" not in imp["filename"] and ".." not in imp["filename"] and imp["kind"] == "imported"
    v = c.post(f"/v1/admin/backup/{imp['id']}/validate?wait=true", json={}, headers=c.admin).json()["operation"]
    assert v["state"] == "succeeded" and v["result"]["ok"], v
    bad = c.post("/v1/admin/backup/import", files={"file": ("x.olcfbak", io.BytesIO(b"not a backup at all"))}, headers=c.admin)
    assert bad.status_code == 400


def test_failed_backup_cleans_up_and_reports(ops_app, monkeypatch):
    from olcf.ops import backup_core as core

    def boom(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(core, "_dump_table", boom)
    r = ops_app.post("/v1/admin/backup?wait=true", json={}, headers=ops_app.admin)
    bk = r.json()["backup"]
    assert bk["state"] == "failed" and "disk full" in str(bk["error"])
    assert not list(ops_app.ctx.settings.path("backups").glob("*.olcfbak*"))


def test_health_check_backup(ops_app):
    c = ops_app
    h = c.get("/v1/admin/health/checks", headers=c.admin).json()
    assert any(x["name"] == "backup" for x in (h["items"] if "items" in h else h.get("checks", [])))
    r = c.post("/v1/admin/health/checks/backup/test", headers=c.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert [s["name"] for s in body["steps"]][:2] == ["dump", "verify"]
