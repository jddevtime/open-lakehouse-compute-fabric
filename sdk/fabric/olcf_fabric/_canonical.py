# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Canonical JSON, digest and signature - byte-for-byte the same construction as the control plane (``olcf.fabric.capsule`` / ``olcf.fabric.signing``)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

MAC_DOMAIN = b"olcf.capsule.v1\x00"


def canonical_json(obj: Any) -> bytes:
    """``json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`` as UTF-8."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha256_hex(data: bytes | str) -> str:
    return hashlib.sha256(data.encode("utf-8") if isinstance(data, str) else data).hexdigest()


def b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def sign_capsule(capsule: dict[str, Any], key_b64: str, key_id: str) -> dict[str, Any]:
    """The detached signature ``{alg, keyId, digest, mac}`` for ``capsule`` using the per-submission key from ``/challenge``."""
    canonical = canonical_json(capsule)
    key = b64url_decode(key_b64)
    return {"alg": "HMAC-SHA256", "keyId": key_id, "digest": "sha256:" + sha256_hex(canonical), "mac": hmac.new(key, MAC_DOMAIN + canonical, hashlib.sha256).hexdigest()}
