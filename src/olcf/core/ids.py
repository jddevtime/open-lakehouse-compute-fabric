# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Identifier helpers.

All platform identifiers are ``<prefix>_<ULID>``: immutable, globally unique and
lexicographically sortable by creation time, which keeps "newest first" listings
cheap on both SQLite and PostgreSQL.
"""

from __future__ import annotations

import os
import threading
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_lock = threading.Lock()
_last_ms = 0
_last_rand = 0

#: Registered identifier prefixes (documentation + validation aid).
PREFIXES = {
    "usr": "user / principal",
    "wks": "workspace",
    "itm": "item",
    "con": "connection",
    "sec": "secret",
    "job": "job",
    "ses": "interactive session",
    "wrk": "worker",
    "pol": "worker pool",
    "rtm": "runtime installation",
    "run": "pipeline run",
    "act": "activity run",
    "evt": "event",
    "aud": "audit record",
    "tok": "api token",
    "sub": "fabric submission",
    "alr": "alert",
    "bkp": "backup",
    "hok": "webhook",
    "qry": "query history entry",
    "cat": "catalog entry",
    "lin": "lineage edge",
    "que": "scheduler queue",
    "clu": "compute cluster (Spark standalone/k8s/...)",
    "prv": "compute provider registration",
    "sap": "Spark application record",
    "att": "job attempt",
    "cmd": "worker command (cross-replica outbox)",
    "bun": "runtime offline bundle",
    "enr": "worker enrollment",
    "idp": "identity provider (OIDC / Entra / LDAP)",
    "bkt": "backup target",
    "upd": "platform update record",
    "sbd": "support bundle",
    "rtn": "retention run",
    "sch": "backup schedule",
    "ast": "intelligence asset (stable internal asset id)",
    "edg": "intelligence graph edge",
    "scn": "intelligence discovery scan (one metadata snapshot)",
    "src": "intelligence discovery source",
    "smp": "intelligence sampling policy",
    "trm": "business glossary term",
    "dom": "governance domain",
    "dpr": "data product",
    "cls": "classification label",
    "chg": "governance change request",
    "plc": "governance policy",
    "vio": "governance policy violation",
}


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def ulid() -> str:
    """Generate a monotonic 26-character ULID."""
    global _last_ms, _last_rand
    with _lock:
        ms = int(time.time() * 1000)
        if ms <= _last_ms:
            ms = _last_ms
            _last_rand += 1
        else:
            _last_rand = int.from_bytes(os.urandom(10), "big")
            _last_ms = ms
        rand = _last_rand & ((1 << 80) - 1)
    return _encode(ms, 10) + _encode(rand, 16)


def new_id(prefix: str) -> str:
    """Return a new identifier such as ``job_01J8...``."""
    if prefix not in PREFIXES:
        raise ValueError(f"unknown id prefix {prefix!r}")
    return f"{prefix}_{ulid()}"


def id_prefix(identifier: str) -> str:
    """Return the prefix of an identifier (``''`` if it has none)."""
    return identifier.split("_", 1)[0] if "_" in identifier else ""
