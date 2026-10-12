# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Challenge / verify against the real database: single-use nonces, principal binding, windows, replay semantics, race, key rotation, caps."""

from __future__ import annotations

import copy
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import update

from olcf.core.errors import TooManyRequests
from olcf.fabric import schema as S
from olcf.fabric import signing
from olcf.fabric.capsule import canonical_json
from olcf.fabric.errors import (
    CapsuleExpired,
    CapsuleReplay,
    CapsuleSignatureInvalid,
    CapsuleTooLarge,
    NonceInvalid,
)
from olcf_fabric import capsule as sdk_cap
from tests.fabric.fabric_stack import FabricStack

MASTER = b"m" * 32


@pytest.fixture()
def svc(stack: FabricStack) -> signing.ChallengeService:
    return signing.ChallengeService(stack.ctx, lambda: MASTER)


def signed_for(ch: dict[str, Any], *, created: str | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    parts = {"source": {"mode": "inline", "content": "x"}, "context": {}, "runtime": {}, "parameters": {"a": 1}, "dataReferences": {}, "policy": {}, "security": {"identity": {}}}
    signed = sdk_cap.build_signed(parts, ch, idempotency_key=None, correlation_id=None)
    if created:
        signed["capsule"]["createdAt"] = created
    if extra:
        signed["capsule"].update(extra)
    if created or extra:
        from olcf_fabric._canonical import sign_capsule
        signed["signature"] = sign_capsule(signed["capsule"], ch["key"], ch["keyId"])
    return signed


def test_issue_and_verify_roundtrip(svc: signing.ChallengeService) -> None:
    ch = svc.issue("usr_a")
    assert ch["alg"] == "HMAC-SHA256" and ch["submissionId"].startswith("sub_") and ch["maxAgeSeconds"] > 0 and ch["keyId"] == signing.key_id_of(MASTER)
    out = svc.verify("usr_a", signed_for(ch))
    assert out["replay"] is None and out["digest"].startswith("sha256:")


def test_every_challenge_is_unique(svc: signing.ChallengeService) -> None:
    a, b = svc.issue("usr_a"), svc.issue("usr_a")
    assert a["nonce"] != b["nonce"] and a["submissionId"] != b["submissionId"] and a["key"] != b["key"]


def test_the_key_is_bound_to_the_principal(svc: signing.ChallengeService) -> None:
    ch = svc.issue("usr_a")
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_b", signed_for(ch))                      # another caller cannot use a stolen challenge
    svc.verify("usr_a", signed_for(ch))                          # the legitimate caller still can: a failed attempt must not consume the nonce


def test_nonce_unknown_or_mismatched(svc: signing.ChallengeService) -> None:
    ch = svc.issue("usr_a")
    other = svc.issue("usr_a")
    s = signed_for(ch)
    s["capsule"]["submissionId"] = other["submissionId"]          # right nonce, another submission's id
    from olcf_fabric._canonical import sign_capsule
    s["signature"] = sign_capsule(s["capsule"], signing._b64(signing.derive_key(MASTER, ch["nonce"], other["submissionId"], "usr_a")), ch["keyId"])  # noqa: SLF001
    with pytest.raises(NonceInvalid):
        svc.verify("usr_a", s)
    with pytest.raises(NonceInvalid):
        svc.verify("usr_a", {"capsule": {"security": {}}, "signature": {}})
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_a", {"nope": 1})


def test_tamper_after_signing(svc: signing.ChallengeService) -> None:
    ch = svc.issue("usr_a")
    s = signed_for(ch)
    t = copy.deepcopy(s)
    t["capsule"]["parameters"]["a"] = 2
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_a", t)
    t2 = copy.deepcopy(s)
    t2["signature"]["keyId"] = "k-other"
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_a", t2)
    t3 = copy.deepcopy(s)
    t3["signature"]["digest"] = "sha256:" + "0" * 64
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_a", t3)
    svc.verify("usr_a", s)


def test_rotation_invalidates_outstanding_challenges(stack: FabricStack) -> None:
    holder = [MASTER]
    svc = signing.ChallengeService(stack.ctx, lambda: holder[0])
    ch = svc.issue("usr_a")
    holder[0] = b"n" * 32
    with pytest.raises(CapsuleSignatureInvalid) as ei:
        svc.verify("usr_a", signed_for(ch))
    assert "rotated" in (ei.value.remediation or "")


def test_window(svc: signing.ChallengeService, stack: FabricStack) -> None:
    ch = svc.issue("usr_a")
    window = int(stack.settings.fabric.capsule_max_age_seconds)
    now = datetime.now(UTC)
    for shift in (-(window + 30), window + 30):
        c = svc.issue("usr_a")
        stamp = (now + timedelta(seconds=shift)).isoformat().replace("+00:00", "Z")
        with pytest.raises(CapsuleExpired):
            svc.verify("usr_a", signed_for(c, created=stamp))
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_a", signed_for(ch, created="not a time"))
    inside = (now - timedelta(seconds=window - 30)).isoformat().replace("+00:00", "Z")
    svc.verify("usr_a", signed_for(ch, created=inside))


def test_expired_challenge(svc: signing.ChallengeService, stack: FabricStack) -> None:
    ch = svc.issue("usr_a")
    with stack.ctx.db.begin() as conn:
        conn.execute(update(S.fabric_nonces).where(S.fabric_nonces.c.nonce == ch["nonce"]).values(expires_at=(datetime.now(UTC) - timedelta(seconds=5)).isoformat()))
    with pytest.raises(NonceInvalid) as ei:
        svc.verify("usr_a", signed_for(ch))
    assert "expired" in str(ei.value)


def test_replay_of_a_different_capsule_is_rejected_and_of_the_same_one_is_idempotent(svc: signing.ChallengeService, stack: FabricStack) -> None:
    ch = svc.issue("usr_a")
    s = signed_for(ch)
    first = svc.verify("usr_a", s)
    with pytest.raises(CapsuleReplay):
        svc.verify("usr_a", s)                                    # used, but no submission row exists for it yet: not a harmless retry
    forged = signed_for(ch, extra={"parameters": {"a": 99}})
    with pytest.raises(CapsuleReplay):
        svc.verify("usr_a", forged)
    # once the submission row exists (same id and digest) an exact retry returns it
    from olcf.core.timeutil import now_iso
    from olcf.db.util import insert_row
    row = {"id": ch["submissionId"], "received_at": now_iso(), "principal_id": "usr_a", "idempotency_key": "k1", "source_mode": "inline", "capsule_bytes": 1, "capsule_sha256": first["digest"],
           "parameters": {}, "state": "queued", "updated_at": now_iso(), "caller": {}}
    with stack.ctx.db.begin() as conn:
        insert_row(conn, S.fabric_submissions, row)
    again = svc.verify("usr_a", s)
    assert again["replay"] is not None and again["replay"]["id"] == ch["submissionId"]
    with pytest.raises(CapsuleReplay):
        svc.verify("usr_a", forged)
    with pytest.raises(CapsuleSignatureInvalid):
        svc.verify("usr_b", s)                                    # another principal gets no replay of someone else's submission (the MAC is bound to the principal)


def test_concurrent_submissions_of_one_nonce_have_exactly_one_winner(svc: signing.ChallengeService) -> None:
    ch = svc.issue("usr_a")
    results: list[str] = []
    gate = threading.Barrier(6)

    def go() -> None:
        s = signed_for(ch)
        gate.wait()
        try:
            svc.verify("usr_a", s)
            results.append("ok")
        except CapsuleReplay:
            results.append("replay")

    threads = [threading.Thread(target=go) for _ in range(6)]
    [t.start() for t in threads]
    [t.join(30) for t in threads]
    assert sorted(results) == ["ok"] + ["replay"] * 5


def test_size_limit(svc: signing.ChallengeService, stack: FabricStack, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stack.settings.fabric, "capsule_max_bytes", 2000)
    ch = svc.issue("usr_a")
    assert ch["maxBytes"] == 2000
    s = signed_for(ch, extra={"padding": "x" * 5000})
    with pytest.raises(CapsuleTooLarge):
        svc.verify("usr_a", s)
    assert len(canonical_json(s["capsule"])) > 2000


def test_open_challenge_cap(svc: signing.ChallengeService, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(signing, "MAX_OPEN_NONCES_PER_PRINCIPAL", 3)
    for _ in range(3):
        svc.issue("usr_flood")
    with pytest.raises(TooManyRequests):
        svc.issue("usr_flood")
    svc.issue("usr_other")                                        # the cap is per principal
