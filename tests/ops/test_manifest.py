# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Signed manifest verification: every way of presenting something unsigned, forged, stale or replayed is rejected."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from olcf.ops import manifest as M


def payload(**kw):
    p = {"schema": 1, "product": "olcf", "channel": "stable", "version": "0.2.0", "sequence": 5, "released_at": "2026-01-01T00:00:00Z", "artifacts": []}
    p.update(kw)
    return p


@pytest.fixture()
def keys():
    priv, raw, kid = M.generate_keypair()
    priv2, raw2, kid2 = M.generate_keypair()
    return {"p1": priv, "k1": M.TrustedKey(kid, raw, "one"), "p2": priv2, "k2": M.TrustedKey(kid2, raw2, "two")}


def test_roundtrip_and_fields(keys):
    env = M.sign_manifest(payload(notes="hi"), keys["p1"])
    v = M.verify_envelope(json.dumps(env), [keys["k1"]], expect_channel="stable")
    assert v.manifest.version == "0.2.0" and v.keyids == [keys["k1"].keyid] and v.manifest.notes == "hi"


def test_unsigned_untrusted_and_revoked(keys):
    env = M.sign_manifest(payload(), keys["p1"])
    with pytest.raises(M.ManifestError, match="unsigned"):
        M.verify_envelope({**env, "signatures": []}, [keys["k1"]])
    with pytest.raises(M.ManifestError, match="not in the trust store"):
        M.verify_envelope(env, [keys["k2"]])
    with pytest.raises(M.ManifestError, match="revoked"):
        M.verify_envelope(env, [M.TrustedKey(keys["k1"].keyid, keys["k1"].public_raw, revoked=True)])
    with pytest.raises(M.ManifestError):
        M.verify_envelope(env, [])


def test_tampered_payload_and_forged_signature(keys):
    env = M.sign_manifest(payload(), keys["p1"])
    evil = dict(payload(version="9.9.9"))
    forged = {"payload": base64.b64encode(M.canonical(evil)).decode(), "signatures": env["signatures"]}
    with pytest.raises(M.ManifestError, match="signature verification failed"):
        M.verify_envelope(forged, [keys["k1"]])
    # a different key claiming the trusted key id
    other = M.sign_manifest(payload(), keys["p2"])
    other["signatures"][0]["keyid"] = keys["k1"].keyid
    with pytest.raises(M.ManifestError):
        M.verify_envelope(other, [keys["k1"]])


def test_domain_separation(keys):
    raw = M.canonical(payload())
    naked = keys["p1"].sign(raw)  # signature over the payload WITHOUT the domain prefix
    env = {"payload": base64.b64encode(raw).decode(), "signatures": [{"keyid": keys["k1"].keyid, "alg": "ed25519", "sig": base64.b64encode(naked).decode()}]}
    with pytest.raises(M.ManifestError):
        M.verify_envelope(env, [keys["k1"]])


def test_threshold_requires_distinct_keys(keys):
    env = M.sign_manifest(payload(), keys["p1"], keys["p1"])  # same key twice does not count twice
    with pytest.raises(M.ManifestError, match="1 of 2"):
        M.verify_envelope(env, [keys["k1"], keys["k2"]], threshold=2)
    env2 = M.sign_manifest(payload(), keys["p1"], keys["p2"])
    assert len(M.verify_envelope(env2, [keys["k1"], keys["k2"]], threshold=2).keyids) == 2


def test_expiry_channel_and_garbage(keys):
    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    with pytest.raises(M.ManifestError, match="expired"):
        M.verify_envelope(M.sign_manifest(payload(expires_at=past), keys["p1"]), [keys["k1"]])
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    M.verify_envelope(M.sign_manifest(payload(expires_at=future), keys["p1"]), [keys["k1"]])
    with pytest.raises(M.ManifestError, match="channel"):
        M.verify_envelope(M.sign_manifest(payload(channel="beta"), keys["p1"]), [keys["k1"]], expect_channel="stable")
    for junk in (b"", b"not json", b"[]", b'{"payload": 1, "signatures": []}', b'{"payload": "!!!", "signatures": [{}]}', b"x" * (M.MAX_ENVELOPE_BYTES + 1)):
        with pytest.raises(M.ManifestError):
            M.verify_envelope(junk, [keys["k1"]])
    with pytest.raises(ValidationError):
        M.sign_manifest(payload(version="not-a-version"), keys["p1"])
    with pytest.raises(ValidationError):
        M.sign_manifest(payload(artifacts=[{"kind": "wheel", "name": "../evil.whl", "sha256": "0" * 64, "size": 1}]), keys["p1"])


def test_unknown_fields_and_alg_none(keys):
    env = M.sign_manifest(payload(), keys["p1"])
    env["signatures"][0]["alg"] = "none"
    with pytest.raises(M.ManifestError):
        M.verify_envelope(env, [keys["k1"]])
    with pytest.raises(ValidationError):
        M.sign_manifest(payload(extra_field=1), keys["p1"])


def test_anti_rollback():
    m = M.Manifest.model_validate(payload(sequence=3, version="0.2.0"))
    M.check_not_rollback(m, installed="0.1.0", floor=None)
    M.check_not_rollback(m, installed="0.1.0", floor={"sequence": 3, "version": "0.2.0"})
    with pytest.raises(M.ManifestError, match="rollback refused"):
        M.check_not_rollback(m, installed="0.1.0", floor={"sequence": 4, "version": "0.2.0"})
    with pytest.raises(M.ManifestError, match="rollback refused"):
        M.check_not_rollback(m, installed="0.1.0", floor={"sequence": 1, "version": "0.3.0"})


def test_cli_keygen_sign_verify(tmp_path, monkeypatch, capsys):
    assert M._cli(["keygen"]) == 0
    k = json.loads(capsys.readouterr().out)
    (tmp_path / "p.json").write_text(json.dumps(payload()))
    monkeypatch.setenv("OLCF_UPDATE_SIGNING_KEY", k["private_key"])
    assert M._cli(["sign", str(tmp_path / "p.json"), "-o", str(tmp_path / "m.json")]) == 0
    assert M._cli(["verify", str(tmp_path / "m.json"), "--public-key", k["public_key"]]) == 0
    other = M.generate_keypair()
    assert M._cli(["verify", str(tmp_path / "m.json"), "--public-key", base64.b64encode(other[1]).decode()]) == 2
