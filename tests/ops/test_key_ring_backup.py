# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Backup and restore keep the key ring consistent with the ciphertext (credential lifecycle).

The ring lives in the database, so a backup carries it in the same snapshot as the secret rows; ``master``-mode backups are encrypted with the
operator-held *root* key, never with a ring key, so a clean machine needs nothing but that root key.
"""

from __future__ import annotations

import shutil

import pytest

from olcf.ops import backup_core as core
from olcf.ops.crypto import KeySpec, read_header
from olcf.security.secrets import FileSecretProvider, MasterKey, SecretStore

SEC = "/v1/admin/security"


@pytest.fixture(autouse=True)
def fast_ring(monkeypatch):
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "0")


def _backup(c, **body):
    r = c.post("/v1/admin/backup?wait=true", json=body, headers=c.admin)
    assert r.status_code == 202, r.text
    return r.json()["backup"]


def _restore_test(c, bid):
    r = c.post(f"/v1/admin/backup/{bid}/restore-test?wait=true", json={}, headers=c.admin)
    assert r.status_code == 200, r.text
    return r.json()["operation"]["result"]


def _rotate(c, kind="master-key", **body):
    r = c.post(f"{SEC}/rotate/{kind}", params={"wait": True}, json=body, headers=c.admin)
    assert r.status_code == 200, r.text
    return r.json()


def test_restore_test_decrypts_secrets_sealed_under_ring_keys(ops_app):
    c = ops_app
    ids = {c.ctx.secrets.create(f"s{i}", f"v{i}")["id"]: f"v{i}" for i in range(3)}
    rot = _rotate(c, reencrypt=False)  # mixed: secrets still under the old key, one new secret under the new one
    new_id = c.ctx.secrets.create("fresh", "fresh-value")["id"]
    ids[new_id] = "fresh-value"
    bk = _backup(c)
    assert bk["key_id"] == c.ctx.secrets.master.backup_id != rot["new_key"]
    res = _restore_test(c, bk["id"])
    assert res["ok"], res
    check = next(x for x in res["checks"] if x["name"] == "secrets-decrypt")
    assert check["ok"] and "restored key ring" in check["detail"]
    assert read_header(c.ctx.settings.path("backups", bk["filename"]))["key_id"] == c.ctx.secrets.master.backup_id


def test_old_backup_still_restores_after_the_old_key_was_dropped(ops_app):
    """A backup taken before ``drop-old`` carries the ring of its time: its secrets stay readable on a node that dropped the key since."""
    c = ops_app
    for i in range(2):
        c.ctx.secrets.create(f"s{i}", f"v{i}")
    before = _backup(c)  # secrets under the original (root) key, no ring yet
    _rotate(c)
    _rotate(c, action="drop-old")
    after = _backup(c)
    for bk in (before, after):
        res = _restore_test(c, bk["id"])
        assert res["ok"], (bk["id"], res)
    assert next(x for x in _restore_test(c, before["id"])["checks"] if x["name"] == "secrets-decrypt")["ok"]


def test_clean_machine_needs_only_the_root_key(ops_app, tmp_path):
    c = ops_app
    ids = {c.ctx.secrets.create(f"s{i}", f"v{i}")["id"]: f"v{i}" for i in range(3)}
    _rotate(c)
    _rotate(c, action="drop-old")
    bk = _backup(c)
    # the clean machine: a new data directory holding nothing but a copy of the root key file
    clean = tmp_path / "clean" / "secrets"
    clean.mkdir(parents=True)
    shutil.copy(c.ctx.settings.path("secrets", "master.key"), clean / "master.key")
    master = MasterKey(clean)
    assert master.current_id == master.backup_id and master.ring.doc is None  # before the restore it knows no ring key
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'clean' / 'olcf.db'}")
    try:
        rep = core.restore_backup(c.ctx.settings.path("backups", bk["filename"]), KeySpec(None, master), db, run_migrations=True)
        assert rep.ok
        restored = master.view_for(db)  # same root key, the ring that came with the backup
        assert restored.current_id != restored.backup_id
        store = SecretStore(db, {"file": FileSecretProvider(restored)}, master=restored)
        for sid, value in ids.items():
            assert store.resolve(sid) == value
    finally:
        db.dispose()


def test_wrong_root_key_is_reported_by_prerequisites_and_restore_test(ops_app, tmp_path):
    c = ops_app
    c.ctx.secrets.create("s", "v")
    _rotate(c)
    bk = _backup(c)
    pre = c.get(f"/v1/admin/backup/{bk['id']}/prerequisites", headers=c.admin).json()["prerequisites"]
    assert next(x for x in pre if x["name"] == "decryption")["ok"] is True
    assert "root master key" in next(x for x in pre if x["name"] == "master key for secrets")["detail"]
    # a machine with a different root key cannot open a master-mode backup, and says why
    other = MasterKey(tmp_path / "other")
    rep = core.validate_backup(c.ctx.settings.path("backups", bk["filename"]), KeySpec(None, other))
    assert not rep.ok and "not available here" in " ".join(rep.errors)
