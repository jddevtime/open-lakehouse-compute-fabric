# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Capsule signing and replay protection (FA-05 / FA-10).

Flow
----
1. The SDK calls ``POST /v1/integrations/fabric/challenge`` (authenticated). The control plane stores a **single-use nonce** (table ``fabric_nonces``) bound to the
   caller and to a fresh ``submissionId``, derives a **per-submission HMAC key** from its signing material and returns ``{nonce, submissionId, key, keyId,
   serverTime, expiresAt}``. The key is never stored: it is re-derived at verification time.
2. The SDK builds the capsule (nonce and keyId inside ``security``), canonicalises it, and sends ``digest = sha256(canonical)`` and
   ``mac = HMAC-SHA256(key, "olcf.capsule.v1\\0" + canonical)``.
3. The control plane re-canonicalises the received capsule, recomputes digest and MAC (constant-time compare), checks the timestamp window
   (``settings.fabric.capsule_max_age_seconds``), the size limit and the nonce (exists, same caller, same submission id, unexpired, **unused**), and marks the nonce used
   atomically. A retry of the *same* submission (same id and digest) after a lost response is answered from the existing record instead of failing.

What this protects - and what it does not
-----------------------------------------
* **Integrity**: a capsule altered after the SDK signed it (by a proxy, a compromised intermediary, a replayed-and-edited request) fails verification.
* **Replay**: a captured request cannot be submitted twice (nonce) nor later than the window allows (timestamp).
* **Not identity**: the key is handed to *any authenticated caller*, so the signature proves "this body is what the key holder signed", not *who* the author is.
  Identity comes from the bearer token (platform token or Entra JWT) and from the integration's tenant/workspace allow-lists. A stolen token can obtain keys and
  sign anything its principal may submit - revoke the token (and tighten the allow-lists) in that case.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update

from olcf.core.errors import TooManyRequests
from olcf.core.ids import ulid
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_one
from olcf.fabric import schema as S
from olcf.fabric.capsule import MAC_DOMAIN, canonical_json, parse_ts
from olcf.fabric.errors import (
    CapsuleExpired,
    CapsuleReplay,
    CapsuleSignatureInvalid,
    CapsuleTooLarge,
    NonceInvalid,
)

MAX_OPEN_NONCES_PER_PRINCIPAL = 500


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def derive_key(master: bytes, nonce: str, submission_id: str, principal_id: str) -> bytes:
    """Per-submission key: ``HMAC(master, "capsule-key" | nonce | submission id | principal id)``."""
    msg = b"\x00".join([b"capsule-key", nonce.encode(), submission_id.encode(), principal_id.encode()])
    return hmac.new(master, msg, hashlib.sha256).digest()


def key_id_of(master: bytes) -> str:
    return "k-" + hashlib.sha256(b"key-id\x00" + master).hexdigest()[:10]


def sign(key: bytes, canonical: bytes) -> str:
    """The MAC the SDK computes (hex)."""
    return hmac.new(key, MAC_DOMAIN + canonical, hashlib.sha256).hexdigest()


def new_submission_id() -> str:
    return "sub_" + ulid()


def _new_nonce() -> str:
    return _b64(os.urandom(32))


class ChallengeService:
    """Issues challenges and verifies signed capsules. ``master`` is a callable returning the signing material (bytes)."""

    def __init__(self, ctx: Any, master: Any) -> None:
        self.ctx = ctx
        self._master = master

    @property
    def settings(self) -> Any:
        return self.ctx.settings.fabric

    # ------------------------------------------------------------------ issue
    def issue(self, principal_id: str) -> dict[str, Any]:
        master = self._master()
        ttl = int(self.settings.capsule_max_age_seconds)
        nonce = _new_nonce()
        submission_id = new_submission_id()
        now = datetime.now(UTC)
        expires = now + timedelta(seconds=ttl)
        with self.ctx.db.begin() as conn:
            open_n = conn.execute(select(func.count()).select_from(S.fabric_nonces).where(
                S.fabric_nonces.c.principal_id == principal_id, S.fabric_nonces.c.used_at.is_(None), S.fabric_nonces.c.expires_at > now.isoformat())).scalar() or 0
            if open_n >= MAX_OPEN_NONCES_PER_PRINCIPAL:
                raise TooManyRequests("too many unused challenges for this caller", remediation="submit or let the outstanding challenges expire, then retry")
            conn.execute(S.fabric_nonces.insert().values(nonce=nonce, principal_id=principal_id, submission_id=submission_id, issued_at=now.isoformat(),
                                                         expires_at=expires.isoformat()))
            if nonce[0] in "AB":  # opportunistic purge of long-expired rows (1/32 of the calls); no scheduler needed
                conn.execute(delete(S.fabric_nonces).where(S.fabric_nonces.c.expires_at < (now - timedelta(days=1)).isoformat()))
        key = derive_key(master, nonce, submission_id, principal_id)
        return {"nonce": nonce, "submissionId": submission_id, "key": _b64(key), "keyId": key_id_of(master), "alg": "HMAC-SHA256", "serverTime": now.isoformat(),
                "expiresAt": expires.isoformat(), "maxAgeSeconds": ttl, "maxBytes": int(self.settings.capsule_max_bytes)}

    # ------------------------------------------------------------------ verify
    def verify(self, principal_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Verify ``{"capsule": {...}, "signature": {...}}`` and consume the nonce. Returns ``{"canonical": bytes, "digest": str, "replay": row|None}``.

        ``replay`` is the existing submission row when this is a harmless retry of an already accepted submission (same id and digest).
        Raises one of the :mod:`olcf.fabric.errors` exceptions otherwise.
        """
        capsule, sig = body.get("capsule"), body.get("signature")
        if not isinstance(capsule, dict) or not isinstance(sig, dict):
            raise CapsuleSignatureInvalid("the body must contain 'capsule' and 'signature'", remediation="sign the capsule with the key from POST /v1/integrations/fabric/challenge")
        canonical = canonical_json(capsule)
        if len(canonical) > int(self.settings.capsule_max_bytes):
            raise CapsuleTooLarge(f"the capsule is {len(canonical)} bytes; the limit is {int(self.settings.capsule_max_bytes)}",
                                  remediation="submit by reference (pull mode: send only the notebook ids) or remove large outputs from the notebook")
        digest = "sha256:" + hashlib.sha256(canonical).hexdigest()
        security = capsule.get("security") if isinstance(capsule.get("security"), dict) else {}
        nonce, submission_id = str(security.get("nonce") or ""), str(capsule.get("submissionId") or "")
        if not nonce or not submission_id:
            raise NonceInvalid("the capsule carries no nonce / submissionId", remediation="request a challenge and place its nonce in security.nonce")
        master = self._master()
        expected_kid = key_id_of(master)
        if sig.get("keyId") != expected_kid or security.get("keyId") != expected_kid:
            raise CapsuleSignatureInvalid("the signing key is not the current one", remediation="request a new challenge (the signing material was rotated)")
        key = derive_key(master, nonce, submission_id, principal_id)
        mac = sign(key, canonical)
        if sig.get("digest") != digest or not hmac.compare_digest(str(sig.get("mac") or ""), mac):
            raise CapsuleSignatureInvalid("the capsule signature does not match its content",
                                          remediation="the capsule changed after it was signed, or it was signed with another challenge's key")
        # timestamp window -------------------------------------------------------------------------
        try:
            created = parse_ts(str(capsule.get("createdAt")))
        except ValueError as exc:
            raise CapsuleSignatureInvalid(f"createdAt is invalid: {exc}") from exc
        skew = abs((datetime.now(UTC) - created).total_seconds())
        if skew > int(self.settings.capsule_max_age_seconds):
            raise CapsuleExpired(f"createdAt is {int(skew)} s away from the server clock; the allowed window is {int(self.settings.capsule_max_age_seconds)} s",
                                 remediation="check the clock of the Fabric session, or sign and submit within the window (server time is returned by /challenge)")
        # nonce -------------------------------------------------------------------------------------
        now = datetime.now(UTC).isoformat()
        with self.ctx.db.begin() as conn:
            row = fetch_one(conn, select(S.fabric_nonces).where(S.fabric_nonces.c.nonce == nonce))
            if row is None or row["principal_id"] != principal_id or row["submission_id"] != submission_id:
                raise NonceInvalid("the nonce is unknown for this caller and submission", remediation="request a challenge with the same credentials you submit with")
            if row["used_at"] is not None:
                existing = fetch_one(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.id == submission_id))
                if existing is not None and existing["capsule_sha256"] == digest and existing["principal_id"] == principal_id:
                    return {"canonical": canonical, "digest": digest, "replay": existing}
                raise CapsuleReplay("this nonce was already used", remediation="request a new challenge for each submission", details={"submission_id": submission_id})
            if row["expires_at"] < now:
                raise NonceInvalid("the challenge expired", remediation="request a new challenge and submit within its lifetime")
            used = conn.execute(update(S.fabric_nonces).where(S.fabric_nonces.c.nonce == nonce, S.fabric_nonces.c.used_at.is_(None))
                                .values(used_at=now_iso(), capsule_sha256=digest)).rowcount
            if used != 1:  # lost the race against a concurrent submit of the same nonce
                raise CapsuleReplay("this nonce was already used", remediation="request a new challenge for each submission")
        return {"canonical": canonical, "digest": digest, "replay": None}
