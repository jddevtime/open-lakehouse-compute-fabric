# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Password hashing with scrypt (stdlib, memory-hard, no native dependency)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets as _secrets

_N, _R, _P = 2**15, 8, 1


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P, maxmem=128 * 1024 * 1024, dklen=32)
    return f"scrypt${_N}${_R}${_P}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def verify_password(password: str, encoded: str | None) -> bool:
    if not encoded:
        # burn comparable time so unknown users are not distinguishable by timing
        hashlib.scrypt(b"x", salt=b"0" * 16, n=_N, r=_R, p=_P, maxmem=128 * 1024 * 1024, dklen=32)
        return False
    try:
        scheme, n, r, p, salt_b64, dk_b64 = encoded.split("$")
        if scheme != "scrypt":
            return False
        salt, expected = base64.b64decode(salt_b64), base64.b64decode(dk_b64)
        dk = hashlib.scrypt(password.encode(), salt=salt, n=int(n), r=int(r), p=int(p), maxmem=128 * 1024 * 1024, dklen=len(expected))
        return hmac.compare_digest(dk, expected)
    except Exception:  # noqa: BLE001
        return False


def generate_password(length: int = 20) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789-_"
    return "".join(_secrets.choice(alphabet) for _ in range(length))
