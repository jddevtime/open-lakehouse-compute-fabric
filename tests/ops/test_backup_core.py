# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Logical backup engine: dump, validate, restore on SQLite; corruption and safety properties."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import select

from olcf.db import schema as S
from olcf.ops import backup_core as bc
from olcf.ops.crypto import BackupCorrupt, KeySpec
from tests.ops.conftest import CANARY, PASSPHRASE


def _dump(ctx, tmp_path, **kw):
    return bc.write_backup(ctx.db, tmp_path / "b.olcfbak", KeySpec(PASSPHRASE), meta={"backup_id": "bkp_test"}, **kw)


def test_dump_validate_restore_roundtrip(populated, tmp_path, target_db):
    ctx = populated
    res = _dump(ctx, tmp_path)
    assert res.rows > 30 and res.size_bytes > 0
    assert "oidc_auth_requests" not in res.tables and "leader_leases" not in res.tables
    rep = bc.validate_backup(tmp_path / "b.olcfbak", KeySpec(PASSPHRASE))
    assert rep.ok, rep.errors
    assert rep.rows == res.rows
    before = bc.table_counts(ctx.db)
    out = bc.restore_backup(tmp_path / "b.olcfbak", KeySpec(PASSPHRASE), target_db)
    assert out.ok
    after = bc.table_counts(target_db)
    for name, n in before.items():
        if name in bc.EPHEMERAL_TABLES:
            continue
        assert after.get(name) == n, name
    # content equality incl. ciphertext bytes and JSON values
    with ctx.db.connect() as a, target_db.connect() as b:
        for t in (S.secrets, S.audit_log, S.kv, S.principals):
            ra = [dict(r._mapping) for r in a.execute(select(t).order_by(*t.primary_key.columns))]
            rb = [dict(r._mapping) for r in b.execute(select(t).order_by(*t.primary_key.columns))]
            assert ra == rb, t.name
    # audit chain verifies on the restored database
    from olcf.security.audit import AuditLog

    assert AuditLog(target_db).verify()["ok"] is True
    # secrets still decrypt with the same master key
    from olcf.security.secrets import SecretStore

    sec = SecretStore(target_db, ctx.secrets.providers, master=ctx.secrets.master)
    sid = ctx.secrets.list()[0]["id"]
    assert sec.resolve(sid) == ctx.secrets.resolve(sid)


def test_backup_contains_no_plaintext_secrets(populated, tmp_path):
    _dump(populated, tmp_path)
    blob = (tmp_path / "b.olcfbak").read_bytes()
    assert CANARY.encode() not in blob
    # decrypted payload holds ciphertext rows, never the plaintext value either
    for rec in bc.iter_records(tmp_path / "b.olcfbak", KeySpec(PASSPHRASE)):
        assert CANARY not in json.dumps(rec)


def test_wrong_passphrase_and_tamper(populated, tmp_path, target_db):
    _dump(populated, tmp_path)
    p = tmp_path / "b.olcfbak"
    with pytest.raises(BackupCorrupt):
        bc.restore_backup(p, KeySpec("wrong passphrase 12345"), target_db)
    data = bytearray(p.read_bytes())
    data[len(data) // 2] ^= 0xFF
    p.write_bytes(bytes(data))
    rep = bc.validate_backup(p, KeySpec(PASSPHRASE))
    assert not rep.ok
    # nothing was applied to the target by the refused restores
    assert bc.table_counts(target_db) == {} or sum(bc.table_counts(target_db).values()) == 0


def test_truncated_backup_is_invalid(populated, tmp_path):
    _dump(populated, tmp_path)
    p = tmp_path / "b.olcfbak"
    p.write_bytes(p.read_bytes()[:-40])
    assert not bc.validate_backup(p, KeySpec(PASSPHRASE)).ok


def test_restore_refuses_non_empty_without_wipe(populated, tmp_path):
    _dump(populated, tmp_path)
    from olcf.core.errors import Conflict

    with pytest.raises(Conflict):
        bc.restore_backup(tmp_path / "b.olcfbak", KeySpec(PASSPHRASE), populated.db, wipe=False)


def test_future_schema_rejected(populated, tmp_path, monkeypatch):
    _dump(populated, tmp_path)
    p = tmp_path / "b.olcfbak"
    # pretend this code only knows older migrations than the backup carries
    orig = bc.compat_errors
    monkeypatch.setattr(bc, "compat_errors", lambda v, reg=None: orig({**v, "core": 999, "ghost": 1}, reg))
    rep = bc.validate_backup(p, KeySpec(PASSPHRASE))
    assert not rep.ok and any("ghost" in e for e in rep.errors) and any("v999" in e for e in rep.errors)


def test_files_roundtrip_and_path_safety(populated, tmp_path, target_db):
    big = (b"0123456789abcdef" * 40_000)  # > one chunk
    src = tmp_path / "srcfile"
    src.write_bytes(big)
    bc.write_backup(populated.db, tmp_path / "f.olcfbak", KeySpec(PASSPHRASE), meta={},
                    files=[bc.FileSpec("keys/secrets/master.key", data=b"MASTERKEY"), bc.FileSpec("keys/pki/ca.key", source=src),
                           bc.FileSpec("config/settings.json", data=b"{}")])
    rep = bc.validate_backup(tmp_path / "f.olcfbak", KeySpec(PASSPHRASE))
    assert rep.ok and set(rep.manifest["files"]) == {"keys/secrets/master.key", "keys/pki/ca.key", "config/settings.json"}
    dest = tmp_path / "restored"
    out = bc.restore_backup(tmp_path / "f.olcfbak", KeySpec(PASSPHRASE), target_db, files_dir=dest)
    assert (dest / "pki" / "ca.key").read_bytes() == big
    assert oct((dest / "pki" / "ca.key").stat().st_mode & 0o777) == "0o600"
    assert (dest / "secrets" / "master.key").read_bytes() == b"MASTERKEY"
    assert "config/settings.json" in out.files_skipped
    # existing files are not clobbered
    (dest / "secrets" / "master.key").write_bytes(b"EXISTING")
    out2 = bc.restore_backup(tmp_path / "f.olcfbak", KeySpec(PASSPHRASE), target_db, files_dir=dest)
    assert (dest / "secrets" / "master.key").read_bytes() == b"EXISTING"
    assert out2.warnings


@pytest.mark.parametrize("bad", ["../evil", "/etc/passwd", "keys/../../evil", ""])
def test_unsafe_archive_paths_refused(bad):
    with pytest.raises(BackupCorrupt):
        bc._safe_rel(bad)  # noqa: SLF001


def test_dump_leaves_no_partial_on_failure(populated, tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(bc, "_dump_table", boom)
    with pytest.raises(RuntimeError):
        _dump(populated, tmp_path)
    assert list(tmp_path.glob("b.olcfbak*")) == []


def test_constant_memory_streaming(populated, tmp_path):
    """Many rows must not be materialised: peak traced memory stays far below the payload size."""
    import tracemalloc

    from sqlalchemy import insert

    rows = [{"key": f"bulk.{i:06d}", "value": {"pad": "x" * 400, "i": i}, "updated_at": "2026-01-01T00:00:00Z"} for i in range(20_000)]
    with populated.db.begin() as conn:
        for i in range(0, len(rows), 2000):
            conn.execute(insert(S.kv), rows[i:i + 2000])
    del rows
    tracemalloc.start()
    res = _dump(populated, tmp_path)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert res.rows >= 20_000
    assert peak < 40 * 1024 * 1024, peak  # payload is ~10 MB uncompressed; peak is a few batches + frames
