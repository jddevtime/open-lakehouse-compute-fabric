# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Credential lifecycle: staged rotation of the master key and the JWT signing key (API, ring semantics, concurrency, propagation, rollback).

Written against the review's "Credential lifecycle" limitation: rotation needs an entry point, a staged overlap, resumable re-encryption that is safe
next to concurrent secret writes, convergence of every control-plane node and a way back.
"""

from __future__ import annotations

import base64
import threading

import pytest

from olcf.core.errors import Conflict
from olcf.security.secrets import FileSecretProvider, MasterKey, SecretStore
from olcf.security.tokens import SigningKeys, TokenService

SEC = "/v1/admin/security"


@pytest.fixture(autouse=True)
def fast_ring(monkeypatch):
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "0")


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def rotate(client, kind: str, **body):
    wait = body.pop("wait", True)
    return client.post(f"{SEC}/rotate/{kind}", params={"wait": wait}, json=body, headers=client.admin)


def user_headers(client, name: str, role: str) -> dict[str, str]:
    r = client.post("/v1/identity/users", json={"name": name, "password": "Correct-Horse-9-battery", "roles": [{"role": role, "scope_type": "org", "scope_id": ""}]},
                    headers=client.admin)
    assert r.status_code == 201, r.text
    r = client.post("/v1/identity/login", json={"username": name, "password": "Correct-Horse-9-battery"})
    assert r.status_code == 200, r.text
    return bearer(r.json()["access_token"])


def seed(client, n: int = 3) -> dict[str, str]:
    out = {}
    for i in range(n):
        meta = client.ctx.secrets.create(f"s{i}", f"value-{i}")
        out[meta["id"]] = f"value-{i}"
    return out


def assert_all_resolve(client, expected: dict[str, str]) -> None:
    for sid, value in expected.items():
        assert client.ctx.secrets.resolve(sid) == value


# --------------------------------------------------------------------------------------------------------------------- permissions and status
def test_status_shows_ids_stages_and_never_key_material(client):
    seed(client)
    r = client.get(f"{SEC}/keys", headers=client.admin)
    assert r.status_code == 200, r.text
    doc = r.json()
    mk, jw = doc["master_key"], doc["jwt_signing_key"]
    assert mk["primary"] == client.ctx.secrets.master.current_id and mk["root_source"] == "file" and mk["ring_version"] == 0
    assert [k["stage"] for k in mk["keys"]] == ["primary"] and mk["keys"][0]["secrets"] == 3 and mk["secrets"]["pending_reencryption"] == 0
    assert jw["primary"] == client.ctx.tokens.keys.current_kid and jw["externally_managed"] is True
    blob = r.text
    master = client.ctx.secrets.master
    for key in master.keys.values():
        assert base64.b64encode(key).decode() not in blob and key.hex() not in blob
    assert client.ctx.tokens.keys.current.decode() not in blob


def test_only_security_admin_and_platform_admin_may_rotate(client):
    assert client.get(f"{SEC}/keys").status_code == 401
    assert client.post(f"{SEC}/rotate/master-key", json={}).status_code == 401
    for name, role in (("a-contrib", "Contributor"), ("a-viewer", "Viewer"), ("a-runtime", "RuntimeAdmin")):
        h = user_headers(client, name, role)
        assert client.get(f"{SEC}/keys", headers=h).status_code == 403, role
        assert client.post(f"{SEC}/rotate/jwt-signing-key", json={}, headers=h).status_code == 403, role
        assert client.post(f"{SEC}/rotate/master-key", json={}, headers=h).status_code == 403, role
    sec = user_headers(client, "a-sec", "SecurityAdmin")
    assert client.get(f"{SEC}/keys", headers=sec).status_code == 200
    assert client.post(f"{SEC}/rotate/jwt-signing-key", json={}, headers=sec).status_code == 200


def test_restricted_token_scopes_cannot_reach_the_key_endpoints(client):
    """The ``write`` scope excludes ``admin.*``; a PAT with it must not rotate keys even for a PlatformAdmin."""
    pat, _rec = client.ctx.tokens.create_pat(admin_id(client), "narrow", scopes=["write"])
    assert client.post(f"{SEC}/rotate/master-key", json={}, headers=bearer(pat)).status_code == 403
    assert client.get(f"{SEC}/keys", headers=bearer(pat)).status_code == 403


def test_rotation_is_audited_without_key_material(client):
    seed(client, 1)
    old = client.ctx.secrets.master.current_id
    assert rotate(client, "master-key").status_code == 200
    new = client.ctx.secrets.master.current_id
    rows = client.ctx.audit.query(limit=50, action="security.master_key.rotated")
    assert rows and rows[0]["details"]["new_key"] == new and rows[0]["details"]["previous_key"] == old and rows[0]["actor_id"]
    assert client.ctx.audit.query(limit=10, action="security.master_key.reencrypted")
    for r in client.ctx.audit.query(limit=200):
        assert base64.b64encode(client.ctx.secrets.master.keys[new]).decode() not in str(r)


# --------------------------------------------------------------------------------------------------------------------- master key
def test_master_rotation_overlap_reencrypt_drop(client):
    expected = seed(client)
    master = client.ctx.secrets.master
    old = master.current_id
    r = rotate(client, "master-key", reencrypt=False)
    assert r.status_code == 200, r.text
    new = r.json()["new_key"]
    assert new != old and master.current_id == new and r.json()["previous_key"] == old
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["master_key"]
    assert {k["kid"]: k["stage"] for k in st["keys"]} == {new: "primary", old: "overlap"}
    assert st["secrets"]["pending_reencryption"] == 3 and st["can_drop_old"] is False
    assert_all_resolve(client, expected)  # the overlap key still decrypts
    created_after = client.ctx.secrets.create("later", "after-rotation")["id"]
    assert {r["id"]: r["key_id"] for r in _rows(client)}[created_after] == new  # new writes use the new key at once
    # nothing may be dropped while secrets are still sealed under the old key
    blocked = client.post(f"{SEC}/rotate/master-key", json={"action": "drop-old"}, headers=client.admin)
    assert blocked.status_code == 409 and "still sealed" in str(blocked.json())
    assert old in master.keys
    done = rotate(client, "master-key", action="reencrypt")
    assert done.status_code == 200, done.text
    op = done.json()["operation"]
    assert op["state"] == "succeeded" and op["result"]["reencrypted"] == 3 and op["result"]["remaining"] == 0
    assert set(client.ctx.secrets.key_usage()) == {new}
    dropped = rotate(client, "master-key", action="drop-old")
    assert dropped.status_code == 200, dropped.text
    assert old in dropped.json()["dropped"] and old not in master.keys and master.current_id == new
    assert_all_resolve(client, expected)
    assert client.ctx.secrets.resolve(created_after) == "after-rotation"
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["master_key"]
    assert {k["kid"]: k["stage"] for k in st["keys"]}[old] == "retired" and st["primary"] == new
    # a fresh process (restart) with the same root key and database decrypts everything through the persisted ring
    again = MasterKey(master.dir)
    again.attach(client.ctx.db)
    assert again.current_id == new and old not in again.keys
    assert SecretStore(client.ctx.db, {"file": FileSecretProvider(again)}, master=again).resolve(next(iter(expected))) == "value-0"


def test_start_with_reencrypt_in_one_call(client):
    expected = seed(client)
    r = rotate(client, "master-key")
    assert r.status_code == 200, r.text
    assert r.json()["operation"]["state"] == "succeeded" and set(client.ctx.secrets.key_usage()) == {r.json()["new_key"]}
    assert_all_resolve(client, expected)
    assert rotate(client, "master-key", action="drop-old").status_code == 200


def test_reencryption_runs_as_a_pollable_operation_with_progress(client):
    seed(client, 5)
    r = client.post(f"{SEC}/rotate/master-key", json={}, headers=client.admin)  # asynchronous
    assert r.status_code == 200, r.text
    oid = r.json()["operation_id"]
    import time

    for _ in range(100):
        op = client.get(f"/v1/operations/{oid}", headers=client.admin).json()
        if op["state"] != "running":
            break
        time.sleep(0.1)
    assert op["state"] == "succeeded" and op["progress"] == 100 and op["kind"] == "security.reencrypt"
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["master_key"]
    assert st["reencryption"]["state"] == "succeeded" and st["reencryption"]["operation_id"] == oid


def _rows(client):
    from sqlalchemy import select

    from olcf.db import schema as S
    from olcf.db.util import fetch_all

    with client.ctx.db.connect() as conn:
        return fetch_all(conn, select(S.secrets.c.id, S.secrets.c.key_id, S.secrets.c.version, S.secrets.c.ciphertext).where(S.secrets.c.deleted_at.is_(None)))


def test_reencryption_never_overwrites_a_concurrent_value_rotation(client, monkeypatch):
    """The classic lost update: the job decrypted the old value, a user rotates the value, the job writes its stale re-sealed copy back."""
    import olcf.security.secrets as sm

    expected = seed(client, 4)
    victim = sorted(expected)[1]
    rotate(client, "master-key", reencrypt=False)
    real_seal = sm.seal
    fired = {"n": 0}

    def racing_seal(master, plaintext):
        out = real_seal(master, plaintext)
        if plaintext == b"value-1" and not fired["n"]:
            fired["n"] += 1
            client.ctx.secrets.rotate(victim, "NEW-VALUE-FROM-A-USER")  # lands between the job's read and its write
        return out

    monkeypatch.setattr(sm, "seal", racing_seal)
    res = client.ctx.secrets.reencrypt_all()
    assert fired["n"] == 1 and res["skipped"] >= 1
    expected[victim] = "NEW-VALUE-FROM-A-USER"
    assert_all_resolve(client, expected)
    # the rotated value was sealed under the new key by the user's own write; nothing is left under the old key
    assert client.ctx.secrets.reencrypt_all()["remaining"] == 0
    assert set(client.ctx.secrets.key_usage()) == {client.ctx.secrets.master.current_id}


def test_reencryption_is_safe_while_secrets_are_written_concurrently(client):
    expected = seed(client, 30)
    rotate(client, "master-key", reencrypt=False)
    stop = threading.Event()
    created: dict[str, str] = {}
    errors: list[Exception] = []

    def writer():
        i = 0
        try:
            while not stop.is_set() and i < 40:
                meta = client.ctx.secrets.create(f"w{i}", f"w-{i}")
                created[meta["id"]] = f"w-{i}"
                sid = sorted(expected)[i % len(expected)]
                client.ctx.secrets.rotate(sid, f"rot-{i}")
                expected[sid] = f"rot-{i}"
                i += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=writer)
    t.start()
    res = client.ctx.secrets.reencrypt_all(batch=5)
    stop.set()
    t.join()
    assert not errors, errors
    res = client.ctx.secrets.reencrypt_all()
    assert res["remaining"] == 0 and not res["failed"]
    assert_all_resolve(client, {**expected, **created})


def test_reencryption_is_resumable(client):
    expected = seed(client, 6)
    rotate(client, "master-key", reencrypt=False)
    calls = {"n": 0}

    def stop_after_two_batches() -> bool:
        calls["n"] += 1
        return calls["n"] > 3

    first = client.ctx.secrets.reencrypt_all(batch=1, should_stop=stop_after_two_batches)
    assert first.get("stopped") and 0 < first["reencrypted"] < 6 and first["remaining"] == 6 - first["reencrypted"]
    second = client.ctx.secrets.reencrypt_all(batch=1)  # a later run (maybe on another node) picks up exactly where it stopped
    assert second["reencrypted"] == 6 - first["reencrypted"] and second["remaining"] == 0
    assert_all_resolve(client, expected)


def test_unreadable_secret_is_reported_and_blocks_drop_old(client):
    from sqlalchemy import update

    from olcf.db import schema as S

    expected = seed(client, 3)
    victim = sorted(expected)[0]
    with client.ctx.db.begin() as conn:  # a row sealed under a key this node does not have (e.g. a wrong OLCF_MASTER_KEY after a restore)
        conn.execute(update(S.secrets).where(S.secrets.c.id == victim).values(key_id="deadbeef"))
    r = rotate(client, "master-key")
    assert r.status_code == 200
    op = r.json()["operation"]
    assert op["state"] == "failed" and op["error"]["details"]["failed"] == [victim], op
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["master_key"]
    assert st["reencryption"]["state"] == "failed" and st["can_drop_old"] is False
    assert client.post(f"{SEC}/rotate/master-key", json={"action": "drop-old"}, headers=client.admin).status_code == 409
    del expected[victim]
    assert_all_resolve(client, expected)  # the others were still re-encrypted
    health = client.get("/v1/admin/health", headers=client.admin, params={"force": True}).json()
    comp = next(c for c in health["components"] if c["name"] == "key-rotation")
    assert comp["status"] == "degraded" and "re-encryption failed" in comp["detail"]


def test_only_one_reencryption_runs_at_a_time(client):
    seed(client, 2)
    from olcf.api.core.rotation import service

    svc = service(client.ctx)
    svc._claim_lock("op_a")  # noqa: SLF001
    r = client.post(f"{SEC}/rotate/master-key", json={"action": "reencrypt"}, headers=client.admin)
    assert r.status_code == 409 and "already running" in r.text
    # a crashed run (no heartbeat) is taken over
    from sqlalchemy import update

    from olcf.db import schema as S

    with client.ctx.db.begin() as conn:
        conn.execute(update(S.kv).where(S.kv.c.key == svc.LOCK_KEY).values(updated_at="2020-01-01T00:00:00Z"))
    r = client.post(f"{SEC}/rotate/master-key", params={"wait": True}, json={"action": "reencrypt"}, headers=client.admin)
    assert r.status_code == 200 and r.json()["operation"]["state"] == "succeeded"


def test_rollback_makes_the_previous_key_primary_and_loses_nothing(client):
    expected = seed(client)
    master = client.ctx.secrets.master
    old = master.current_id
    new = rotate(client, "master-key").json()["new_key"]
    assert master.current_id == new
    r = rotate(client, "master-key", action="rollback")
    assert r.status_code == 200, r.text
    assert r.json()["primary"] == old and master.current_id == old and r.json()["operation"]["state"] == "succeeded"
    assert set(client.ctx.secrets.key_usage()) == {old}  # re-encrypted back
    assert_all_resolve(client, expected)
    # the demoted key is an overlap key now and can be dropped after the re-encryption
    assert rotate(client, "master-key", action="drop-old").status_code == 200
    assert new not in master.keys
    assert_all_resolve(client, expected)


def test_rollback_without_history_is_refused(client):
    r = rotate(client, "master-key", action="rollback")
    assert r.status_code == 409 and "nothing to roll back" in r.text


def test_concurrent_rotations_never_lose_a_key(client):
    """20 rounds of 4 simultaneous rotations through the real secret store: the ring ends with every key, in one row (see test_keyring_concurrency.py for
    the ring-level, multi-process and PostgreSQL variants)."""
    from sqlalchemy import select

    from olcf.db import schema as S

    ring = client.ctx.secrets.master.ring
    issued: list[str] = []
    errors: list[Exception] = []
    for rnd in range(20):
        def go(gate=threading.Barrier(4)):
            try:
                gate.wait(10)
                issued.append(client.ctx.secrets.master.rotate_ring("tester")[0])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=go) for _ in range(4)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert not errors, errors
        doc = ring._load_doc()  # noqa: SLF001
        assert len(doc["keys"]) == 4 * (rnd + 1), f"round {rnd}: a key was lost"
        assert doc["version"] == 4 * (rnd + 1) and doc["current"] in {k["kid"] for k in doc["keys"]}
        assert set(issued) == {k["kid"] for k in doc["keys"]}
    # old versions were pruned: exactly one row per ring
    with client.ctx.db.connect() as conn:
        assert len(conn.execute(select(S.kv.c.key).where(S.kv.c.key.like("security.keyring.master.0%"))).all()) == 1


# --------------------------------------------------------------------------------------------------------------------- multi-controller propagation
def second_node(client):
    """What another control-plane process looks like: same database, same root key material (shared env or file), its own in-memory state."""
    secrets_dir = client.ctx.settings.path("secrets")
    master = MasterKey(secrets_dir)
    keys = SigningKeys(secrets_dir, master=master)
    return (SecretStore(client.ctx.db, {"file": FileSecretProvider(master)}, master=master),
            TokenService(client.ctx.db, keys, access_ttl=client.ctx.tokens.access_ttl))


def test_other_nodes_converge_without_restart_or_file_copy(client, monkeypatch):
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "3600")  # nothing is picked up by the timer: only the on-demand paths below can converge
    expected = seed(client)
    store2, tokens2 = second_node(client)
    old_master, old_jwt = store2.master.current_id, tokens2.keys.current_kid
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "0")
    new_master = rotate(client, "master-key").json()["new_key"]
    new_jwt = rotate(client, "jwt-signing-key").json()["new_key"]
    monkeypatch.setenv("OLCF_KEYRING_REFRESH_SECONDS", "3600")
    assert store2.master.current_id == old_master  # has not looked yet
    # a secret re-sealed by node 1 under the new key is readable on node 2 at once (unknown key id -> immediate ring re-read)
    for sid, value in expected.items():
        assert store2.resolve(sid) == value
    assert store2.master.current_id == new_master
    # a token signed with the new key (unknown kid on node 2) verifies there, and node 2 now signs with the new key too
    row = client.ctx.service("identity").get(admin_id(client))
    tok, _ = client.ctx.tokens.issue_access_token(row)
    claims = tokens2.verify_access_token(tok)
    assert claims["sub"] == row["id"] and tokens2.keys.current_kid == new_jwt != old_jwt
    # and the reverse: node 2 writes under the new key, node 1 reads it
    sid2 = store2.create("from-node-2", "hello")["id"]
    assert client.ctx.secrets.resolve(sid2) == "hello" and {r["id"]: r["key_id"] for r in _rows(client)}[sid2] == new_master


def test_ring_adopts_a_restored_database_even_with_the_same_version(client):
    """After a restore the ring in the database may be an older one with the same version number: a different document must be adopted."""
    ring = client.ctx.secrets.master.ring
    client.ctx.secrets.master.rotate_ring("t")
    doc = ring._load_doc()  # noqa: SLF001
    from sqlalchemy import update

    from olcf.db import schema as S

    other = dict(doc)
    other["history"] = []
    other["current"] = doc["keys"][0]["kid"]
    with client.ctx.db.begin() as conn:
        conn.execute(update(S.kv).where(S.kv.c.key.like("security.keyring.master.0%")).values(value=other))
    assert ring.refresh(force=True, min_interval=0) and ring.history() == []


def test_missing_root_key_is_reported_not_silently_ignored(client):
    rotate(client, "master-key")
    ring = client.ctx.secrets.master.ring
    doc = ring._load_doc()  # noqa: SLF001
    from sqlalchemy import update

    from olcf.db import schema as S

    broken = dict(doc)
    broken["keys"] = [dict(k, wrapped_by="0badc0de") for k in doc["keys"]]  # as if wrapped by a root key this node does not hold
    with client.ctx.db.begin() as conn:
        conn.execute(update(S.kv).where(S.kv.c.key.like("security.keyring.master.0%")).values(value=broken))
    ring.refresh(force=True, min_interval=0)
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["master_key"]
    assert st["errors"] and "0badc0de" in st["errors"][0]
    assert next(k for k in st["keys"] if k["source"] == "ring")["available"] is False
    health = client.get("/v1/admin/health", headers=client.admin, params={"force": True}).json()
    assert next(c for c in health["components"] if c["name"] == "key-rotation")["status"] == "degraded"


# --------------------------------------------------------------------------------------------------------------------- root key rotation (rewrap)
def test_rewrap_moves_the_ring_under_a_new_root_key(client):
    expected = seed(client)
    master = client.ctx.secrets.master
    old_root = master.backup_id
    new = rotate(client, "master-key").json()["new_key"]
    assert rotate(client, "master-key", action="drop-old").status_code == 200
    # the operator introduces a new root key next to the old one (file mode here; OLCF_MASTER_KEY="<old> <new>" in env mode)
    new_root = master.rotate()
    assert master.backup_id == new_root != old_root
    r = rotate(client, "master-key", action="rewrap")
    assert r.status_code == 200, r.text
    assert r.json()["rewrapped"]["master-key"] == 1 and r.json()["root_key"] == new_root
    doc = master.ring._load_doc()  # noqa: SLF001
    assert {k["wrapped_by"] for k in doc["keys"]} == {new_root}
    # old root removed: the platform restarts with only the new root and still reads everything
    master.drop_old()
    again = MasterKey(master.dir)
    again.attach(client.ctx.db)
    assert list(again.root_keys) == [new_root] and again.current_id == new
    store = SecretStore(client.ctx.db, {"file": FileSecretProvider(again)}, master=again)
    for sid, value in expected.items():
        assert store.resolve(sid) == value


def test_rewrap_without_a_ring_is_refused(client):
    r = rotate(client, "master-key", action="rewrap")
    assert r.status_code == 409


# --------------------------------------------------------------------------------------------------------------------- JWT signing key
def admin_id(client) -> str:
    return client.get("/v1/identity/me", headers=client.admin).json()["principal"]["id"]


def jwt_token(client, ttl=None):
    row = client.ctx.service("identity").get(admin_id(client))
    return client.ctx.tokens.issue_access_token(row, ttl=ttl)[0]


def test_jwt_rotation_keeps_live_tokens_valid_until_drop(client, monkeypatch):
    from olcf.api.core import rotation

    old_tok = jwt_token(client)
    old_kid = client.ctx.tokens.keys.current_kid
    r = rotate(client, "jwt-signing-key")
    assert r.status_code == 200, r.text
    new_kid = r.json()["new_key"]
    assert client.ctx.tokens.keys.current_kid == new_kid != old_kid
    new_tok = jwt_token(client)
    import jwt as pyjwt

    assert pyjwt.get_unverified_header(new_tok)["kid"] == new_kid and pyjwt.get_unverified_header(old_tok)["kid"] == old_kid
    assert client.get("/v1/identity/me", headers=bearer(old_tok)).status_code == 200  # signed with the demoted key: overlap
    assert client.get("/v1/identity/me", headers=bearer(new_tok)).status_code == 200
    st = client.get(f"{SEC}/keys", headers=client.admin).json()["jwt_signing_key"]
    assert {k["kid"]: k["stage"] for k in st["keys"]}[old_kid] == "overlap" and st["can_drop_old"] is False and st["drop_old_eligible_at"]
    # dropping before the longest token lifetime has passed is refused unless forced
    blocked = client.post(f"{SEC}/rotate/jwt-signing-key", json={"action": "drop-old"}, headers=client.admin)
    assert blocked.status_code == 409 and "force" in blocked.text
    monkeypatch.setattr(rotation.KeyRotationService, "max_token_lifetime", lambda self: -1)
    ok = client.post(f"{SEC}/rotate/jwt-signing-key", json={"action": "drop-old"}, headers=client.admin)
    assert ok.status_code == 200 and old_kid in ok.json()["dropped"] and ok.json()["forced"] is False
    assert client.get("/v1/identity/me", headers=bearer(old_tok)).status_code == 401  # the old key is gone
    assert client.get("/v1/identity/me", headers=bearer(new_tok)).status_code == 200


def test_jwt_forced_drop_signs_users_out(client):
    tok = jwt_token(client)
    rotate(client, "jwt-signing-key")
    r = client.post(f"{SEC}/rotate/jwt-signing-key", json={"action": "drop-old", "force": True}, headers=client.admin)
    assert r.status_code == 200 and r.json()["forced"] is True
    assert client.get("/v1/identity/me", headers=bearer(tok)).status_code == 401


def test_jwt_rollback_restores_the_previous_signing_key(client):
    old_kid = client.ctx.tokens.keys.current_kid
    tok_old = jwt_token(client)
    new_kid = rotate(client, "jwt-signing-key").json()["new_key"]
    tok_new = jwt_token(client)
    r = rotate(client, "jwt-signing-key", action="rollback")
    assert r.status_code == 200 and r.json()["primary"] == old_kid
    assert client.ctx.tokens.keys.current_kid == old_kid
    assert client.get("/v1/identity/me", headers=bearer(tok_old)).status_code == 200
    assert client.get("/v1/identity/me", headers=bearer(tok_new)).status_code == 200  # signed with the demoted new key: still verifies
    assert new_kid in {k["kid"] for k in client.ctx.tokens.keys.key_entries() if k["stage"] == "overlap"}


def test_jwt_reencrypt_action_is_rejected(client):
    assert rotate(client, "jwt-signing-key", action="reencrypt").status_code == 400


def test_legacy_file_rotation_still_works_and_is_refused_once_a_ring_exists(client):
    keys = client.ctx.tokens.keys
    k0 = keys.current
    keys.rotate()
    assert keys.current != k0 and keys.previous == k0
    rotate(client, "jwt-signing-key")
    with pytest.raises(Exception, match="key ring"):
        keys.rotate()


def test_legacy_master_key_apis_still_work(client):
    master = client.ctx.secrets.master
    expected = seed(client)
    old = master.current_id
    new = master.rotate()
    assert master.current_id == new != old
    assert_all_resolve(client, expected)
    assert client.ctx.secrets.reencrypt_all()["remaining"] == 0
    master.drop_old()
    assert list(master.keys) == [new]
    assert_all_resolve(client, expected)


# --------------------------------------------------------------------------------------------------------------------- externally managed keys
@pytest.fixture()
def env_keys(monkeypatch):
    k1 = base64.b64encode(bytes(range(32))).decode()
    monkeypatch.setenv("OLCF_MASTER_KEY", k1)
    monkeypatch.setenv("OLCF_JWT_SIGNING_KEY", "env-signing-key-" + "x" * 40)
    return k1


def test_env_provided_keys_work_are_reported_external_and_can_be_rotated_away(env_keys, client):
    expected = seed(client)
    st = client.get(f"{SEC}/keys", headers=client.admin).json()
    mk, jw = st["master_key"], st["jwt_signing_key"]
    assert mk["root_source"] == "env" and mk["externally_managed"] is True and mk["keys"][0]["source"] == "env" and mk["keys"][0]["externally_managed"] is True
    assert jw["root_source"] == "env" and jw["keys"][0]["source"] == "env"
    env_master, env_jwt = mk["primary"], jw["primary"]
    env_token = jwt_token(client)
    # rotate both: the ring lives next to the env keys (wrapped by the env root key)
    assert rotate(client, "master-key").status_code == 200
    assert rotate(client, "jwt-signing-key").status_code == 200
    assert client.ctx.secrets.master.current_id != env_master and client.ctx.tokens.keys.current_kid != env_jwt
    assert_all_resolve(client, expected)
    assert client.get("/v1/identity/me", headers=bearer(env_token)).status_code == 200
    # drop-old retires the env keys for data / verification; a restart with the same env still honours the ring's retirement list
    assert rotate(client, "master-key", action="drop-old").status_code == 200
    r = client.post(f"{SEC}/rotate/jwt-signing-key", json={"action": "drop-old", "force": True}, headers=client.admin)
    assert r.status_code == 200
    assert env_master not in client.ctx.secrets.master.keys and env_master in client.ctx.secrets.master.root_keys  # still the wrapping (root) key
    assert client.get("/v1/identity/me", headers=bearer(env_token)).status_code == 401
    store2, tokens2 = second_node(client)  # "restart": the env still carries the old keys
    assert env_master not in store2.master.keys and store2.master.current_id == client.ctx.secrets.master.current_id
    assert tokens2.keys.current_kid == client.ctx.tokens.keys.current_kid and env_jwt not in {k for k, _ in tokens2.keys.verification_keys()}
    for sid, value in expected.items():
        assert store2.resolve(sid) == value


def test_comma_or_space_separated_env_keys_are_all_roots(monkeypatch, tmp_path):
    a, b = base64.b64encode(bytes(32)).decode(), base64.b64encode(bytes([1] * 32)).decode()
    monkeypatch.setenv("OLCF_MASTER_KEY", f"{a}, {b}")
    m = MasterKey(tmp_path)
    assert len(m.root_keys) == 2 and m.current_id == m.backup_id == MasterKey._kid(bytes([1] * 32))  # noqa: SLF001


def test_master_mode_backup_key_is_the_root_key_not_a_ring_key(client):
    from olcf.ops.crypto import KeySpec

    master = client.ctx.secrets.master
    root = master.backup_id
    rotate(client, "master-key")
    hdr, _ = KeySpec(None, master).for_writing()
    assert master.current_id != root and hdr["key_id"] == root
    assert rotate(client, "master-key", action="drop-old").status_code == 200
    hdr, key = KeySpec(None, master).for_writing()  # the root key is retired as a data key but still derives backups
    assert hdr["key_id"] == root and KeySpec(None, master).for_reading(hdr) is not None


def test_concurrent_start_attempt_conflict_is_a_409(client, monkeypatch):
    # a rotation losing the compare-and-swap six times in a row surfaces as a retryable conflict, not a 500
    ring = client.ctx.secrets.master.ring
    monkeypatch.setattr(ring, "_write", lambda conn, doc, expected: False)
    r = rotate(client, "master-key")
    assert r.status_code == 409 and "retry" in r.text
    with pytest.raises(Conflict):
        client.ctx.secrets.master.rotate_ring("x")
