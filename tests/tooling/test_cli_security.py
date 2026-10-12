# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf security keys|rotate`` and the SDK ``security`` resource: staged key rotation against a real server process / the in-process app."""

from __future__ import annotations

import time

import pytest

from olcf.sdk.errors import ConflictError, NotFoundError


def test_cli_keys_lists_ids_without_material(live_server):
    doc = live_server.json("security", "keys")
    mk, jw = doc["master_key"], doc["jwt_signing_key"]
    assert mk["primary"] and jw["primary"] and mk["keys"][0]["stage"] == "primary"
    key_file = (live_server.data_dir / "secrets" / "master.key").read_text().strip()
    assert key_file not in str(doc)
    human = live_server.invoke("security", "keys").output
    assert "Master key" in human and "JWT signing key" in human and mk["primary"] in human


def test_cli_rotate_master_key_end_to_end(live_server):
    before = live_server.json("security", "keys")["master_key"]["primary"]
    out = live_server.json("security", "rotate", "master-key")
    assert out["new_key"] != before and out["previous_key"] == before and out["operation"]["state"] == "succeeded"
    st = live_server.json("security", "keys")["master_key"]
    assert st["primary"] == out["new_key"] and {k["kid"]: k["stage"] for k in st["keys"]}[before] == "overlap"
    # drop-old and rollback ask for confirmation in a non-interactive session
    res = live_server.invoke("security", "rotate", "master-key", "--action", "drop-old")
    assert res.exit_code != 0 and "--yes" in res.output
    time.sleep(7.5)  # the server waits ring-refresh + 2 s after a rotation so that every node has adopted the new primary key before an old key may go
    dropped = live_server.json("security", "rotate", "master-key", "--action", "drop-old", "--yes")
    assert before in dropped["dropped"]
    gone = live_server.invoke("security", "rotate", "master-key", "--action", "rollback", "--yes")
    assert gone.exit_code == 5 and "not available for rollback" in gone.output  # a dropped key cannot come back
    again = live_server.invoke("security", "rotate", "master-key", "--no-reencrypt", "--yes")
    assert again.exit_code == 0, again.output
    back = live_server.json("security", "rotate", "master-key", "--action", "rollback", "--no-reencrypt", "--yes")
    assert back["primary"] == out["new_key"]


def test_cli_rotate_jwt_signing_key_and_conflicts(live_server):
    out = live_server.json("security", "rotate", "jwt-signing-key")
    assert out["new_key"] and out["drop_old_after_seconds"] > 0
    # the previous key may still verify live tokens: dropping it needs --force
    res = live_server.invoke("security", "rotate", "jwt-signing-key", "--action", "drop-old", "--yes")
    assert res.exit_code == 6, res.output  # conflict
    forced = live_server.invoke("--json", "security", "rotate", "jwt-signing-key", "--action", "drop-old", "--force", "--yes")
    assert forced.exit_code == 0, forced.output


def test_sdk_security_resource(client):
    st = client.security.keys()
    assert st["master_key"]["keys"][0]["stage"] == "primary" and st["refresh_interval_seconds"] >= 0
    with pytest.raises(ConflictError):  # nothing was rotated yet: there is no old key to drop
        client.security.rotate_jwt_signing_key("drop-old")
    r = client.security.rotate_master_key(wait=True)
    assert r["operation"]["state"] == "succeeded" and r["new_key"] == client.security.keys()["master_key"]["primary"]
    started = client.security.rotate_jwt_signing_key()
    assert started["new_key"] and started["previous_key"]
    with pytest.raises(ConflictError, match="signing key"):  # live tokens of the previous key
        client.security.rotate_jwt_signing_key("drop-old")
    dropped = client.security.rotate_jwt_signing_key("drop-old", force=True)
    assert started["previous_key"] in dropped["dropped"] and dropped["forced"] is True
    with pytest.raises(NotFoundError):  # a dropped key cannot come back
        client.security.rotate("jwt-signing-key", "rollback")
