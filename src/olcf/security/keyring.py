# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Versioned key rings kept in the database (staged rotation of the master key and the JWT signing key).

Why a ring in the database
--------------------------
The keys of a node normally come from the environment (``OLCF_MASTER_KEY``, ``OLCF_JWT_SIGNING_KEY``) or from files below ``<data_dir>/secrets``.
Neither can be changed by an API call and neither reaches the *other* control-plane nodes.  A ring is the shared, versioned list of keys that
the nodes converge on:

* ``kv`` rows ``security.keyring.<name>.<version:08d>``; the newest version is the ring.  A write inserts version ``n + 1``: the primary key makes
  that a compare-and-swap, so two administrators (or two nodes) rotating at the same time cannot both win.  Older versions are deleted after a
  successful write, so a dropped key does not linger in an old row.
* Every key of the ring is stored **wrapped** (AES-256-GCM, additional data ``olcf-keyring:<name>:<kid>``) by the *root* master key, i.e. the
  externally provided ``OLCF_MASTER_KEY`` / key-ring / ``master.key``.  The database never holds a key in the clear, and a node that has the root
  key can unwrap everything.  Rotating the root key itself is ``rewrap`` after the operator has added the new root next to the old one.
* Stages: ``primary`` (signs / encrypts new data), ``overlap`` (still verifies / decrypts, nothing new uses it), and - only in the ``retired`` list
  of the document - external keys that nodes must stop trusting.  ``drop-old`` removes the overlap keys and retires the external ones; a rollback
  makes a previous key primary again.
* Nodes re-read the ring at most every :data:`REFRESH_SECONDS` (and at once when they meet a key id they do not know), so a rotation reaches every
  node within that interval without a restart.

The ring never contains anything but wrapped key material and bookkeeping (key ids, stages, timestamps, who did it); the status views built from
it (:meth:`Ring.entries`) carry no key material at all.
"""

from __future__ import annotations

import base64
import copy
import os
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import Conflict, NotFound, Unavailable
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database

PREFIX = "security.keyring."
HISTORY_LIMIT = 50


def refresh_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get("OLCF_KEYRING_REFRESH_SECONDS", "5")))
    except ValueError:
        return 5.0


class Wrapper(Protocol):
    """Wraps key material under the root key (provided by :class:`olcf.security.secrets.MasterKey`)."""

    def wrap(self, plaintext: bytes, aad: bytes) -> tuple[str, str]: ...
    def unwrap(self, blob: str, wrapped_by: str, aad: bytes) -> bytes: ...
    def wrapper_id(self) -> str: ...


def _aad(name: str, kid: str) -> bytes:
    return f"olcf-keyring:{name}:{kid}".encode()


def wrap_with(key: bytes, plaintext: bytes, aad: bytes) -> str:
    nonce = os.urandom(12)
    return base64.b64encode(nonce + AESGCM(key).encrypt(nonce, plaintext, aad)).decode()


def unwrap_with(key: bytes, blob: str, aad: bytes) -> bytes:
    raw = base64.b64decode(blob)
    return AESGCM(key).decrypt(raw[:12], raw[12:], aad)


class Ring:
    """One key ring (``master`` or ``jwt``): load, converge, rotate, promote, drop, rewrap.  Thread-safe."""

    def __init__(self, name: str, wrapper: Wrapper, kid_fn: Callable[[bytes], str]) -> None:
        self.name = name
        self.wrapper = wrapper
        self.kid_fn = kid_fn
        self.db: Database | None = None
        self.doc: dict[str, Any] | None = None
        self.keys: dict[str, bytes] = {}
        self.errors: list[str] = []
        self.gen = 0  # bumped whenever a different ring is adopted; views derived from the ring rebuild on change
        self._loaded_at = float("-inf")
        self._forced_at = float("-inf")
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ persistence
    def attach(self, db: Database) -> None:
        """Bind the ring to a database (the first attachment wins, so helper stores built on the same key objects cannot hijack it)."""
        with self._lock:
            if self.db is None:
                self.db = db
        self.refresh(force=True, min_interval=0)

    def _latest_row(self, conn: Any) -> tuple[str, dict[str, Any]] | None:
        row = conn.execute(select(S.kv.c.key, S.kv.c.value).where(S.kv.c.key.like(PREFIX + self.name + ".%")).order_by(S.kv.c.key.desc()).limit(1)).first()
        return (str(row[0]), dict(row[1])) if row and row[1] else None

    def _load_doc(self) -> dict[str, Any] | None:
        assert self.db is not None
        with self.db.connect() as conn:
            latest = self._latest_row(conn)
        return latest[1] if latest else None

    def refresh(self, *, force: bool = False, min_interval: float | None = None) -> bool:
        """Re-read the ring when it is older than the refresh interval (or ``force``).  Returns True when a newer version was adopted.

        ``force`` is throttled to once per second so a stream of tokens with unknown key ids cannot turn into a stream of database reads.
        """
        if self.db is None:
            return False
        now = time.monotonic()
        with self._lock:
            if force:
                throttle = 1.0 if min_interval is None else min_interval
                if throttle > 0:
                    if now - self._forced_at < throttle:
                        return False
                    self._forced_at = now
            elif now - self._loaded_at < refresh_seconds():
                return False
            self._loaded_at = now
            try:
                doc = self._load_doc()
            except Exception:  # noqa: BLE001 - table missing (not yet migrated) or a database hiccup: keep what we have
                return False
            if doc == self.doc:
                return False  # also catches a database restored to a *different* ring with the same version number
            self._adopt(doc)
            return True

    def _adopt(self, doc: dict[str, Any] | None) -> None:
        keys: dict[str, bytes] = {}
        errors: list[str] = []
        for e in (doc or {}).get("keys", []):
            try:
                keys[e["kid"]] = self.wrapper.unwrap(e["wrapped"], e["wrapped_by"], _aad(self.name, e["kid"]))
            except Exception as exc:  # noqa: BLE001 - root key that wrapped it is not available here
                errors.append(f"{e['kid']}: cannot unwrap with root key {e.get('wrapped_by')} ({type(exc).__name__})")
        self.keys, self.errors, self.doc = keys, errors, doc
        self.gen += 1

    # ------------------------------------------------------------------ views
    @property
    def version(self) -> int:
        return self.doc["version"] if self.doc else 0

    @property
    def current(self) -> str | None:
        return self.doc["current"] if self.doc else None

    @property
    def retired(self) -> frozenset[str]:
        return frozenset(self.doc.get("retired", [])) if self.doc else frozenset()

    def entries(self, external: dict[str, dict[str, Any]], primary: str | None = None) -> list[dict[str, Any]]:
        """Key listing for the status API: ring keys plus ``external`` ones (``kid -> {"source": ...}``).  No key material."""
        out: list[dict[str, Any]] = []
        cur = self.current or primary
        for e in (self.doc or {}).get("keys", []):
            out.append({"kid": e["kid"], "stage": "primary" if e["kid"] == cur else "overlap", "source": "ring", "created_at": e.get("created_at"),
                        "stage_since": e.get("stage_since"), "created_by": e.get("created_by"), "wrapped_by": e.get("wrapped_by"),
                        "available": e["kid"] in self.keys})
        for kid, info in external.items():
            stage = "retired" if kid in self.retired else ("primary" if kid == cur else "overlap")
            out.append({"kid": kid, "stage": stage, "source": info.get("source", "external"), "created_at": None, "stage_since": None,
                        "externally_managed": True, "available": True})
        order = {"primary": 0, "overlap": 1, "retired": 2}
        out.sort(key=lambda r: (order[r["stage"]], str(r.get("created_at") or "")))
        return out

    def history(self) -> list[dict[str, Any]]:
        return list((self.doc or {}).get("history", []))

    # ------------------------------------------------------------------ mutation (read-modify-write in ONE serialized transaction)
    def _write(self, conn: Any, doc: dict[str, Any], expected: int) -> bool:
        """Store ``doc`` as version ``expected + 1`` and prune older versions, if ``expected`` is still the latest version (``conn`` holds the ring lock)."""
        latest = self._latest_row(conn)
        if (int(latest[0].rsplit(".", 1)[1]) if latest else 0) != expected:
            return False
        doc["version"] = expected + 1
        key = f"{PREFIX}{self.name}.{doc['version']:08d}"
        try:
            with conn.begin_nested():
                conn.execute(S.kv.insert().values(key=key, value=doc, updated_at=now_iso()))
        except IntegrityError:
            return False  # backstop: the primary key (the version) exists already
        conn.execute(delete(S.kv).where(S.kv.c.key.like(PREFIX + self.name + ".%"), S.kv.c.key < key))
        return True

    def _mutate(self, fn: Callable[[dict[str, Any] | None], dict[str, Any]], *, attempts: int = 6) -> dict[str, Any]:
        """Apply ``fn`` to the latest committed ring and store the result as the next version.

        Load, compute and store run inside one transaction that holds the ring's mutual-exclusion lock (:meth:`Database.begin_serialized`: PostgreSQL advisory
        lock, SQLite ``BEGIN IMMEDIATE``), so concurrent rotations - threads, processes, control-plane nodes - are applied one after the other and every one
        sees the ring the previous one committed.  An insert-only compare-and-swap is not enough: old versions are pruned, so a writer that read version n
        and was delayed while n+1 and n+2 were written finds the key ``n+1`` free again and would silently overwrite the lineage (its key lost behind n+2).
        The version check in :meth:`_write` stays as a backstop (databases without the lock, a ring restored underneath us): after ``attempts`` lost
        rounds the caller gets a retryable ``Conflict``.  ``fn`` must be repeatable.
        """
        if self.db is None:
            raise Unavailable("the key ring needs the database; key rotation is not available on this node")
        for _ in range(attempts):
            with self.db.begin_serialized(f"olcf.keyring.{self.name}") as conn:
                latest = self._latest_row(conn)  # the committed latest, never the cache
                doc = latest[1] if latest else None
                new = fn(copy.deepcopy(doc) if doc else None)
                stored = self._write(conn, new, int(latest[0].rsplit(".", 1)[1]) if latest else 0)
            if stored:
                with self._lock:
                    self._adopt(new)
                    self._loaded_at = time.monotonic()
                return new
        raise Conflict("another key operation is in progress; retry in a moment")

    @staticmethod
    def _note(doc: dict[str, Any], by: str | None, action: str, **detail: Any) -> None:
        doc.setdefault("history", []).append({"at": now_iso(), "by": by, "action": action, **detail})
        del doc["history"][:-HISTORY_LIMIT]

    def _new_entry(self, material: bytes, by: str | None, now: str) -> dict[str, Any]:
        kid = self.kid_fn(material)
        blob, wrapped_by = self.wrapper.wrap(material, _aad(self.name, kid))
        return {"kid": kid, "wrapped": blob, "wrapped_by": wrapped_by, "created_at": now, "stage_since": now, "created_by": by}

    def rotate(self, by: str | None, *, external_current: str | None, material: bytes | None = None) -> tuple[str, str | None]:
        """Generate a key and make it primary.  The previous primary stays (overlap).  Returns ``(new_kid, previous_kid)``."""
        result: dict[str, str | None] = {}

        def fn(doc: dict[str, Any] | None) -> dict[str, Any]:
            now = now_iso()
            entry = self._new_entry(material or AESGCM.generate_key(256), by, now)
            new = dict(doc) if doc else {"name": self.name, "keys": [], "retired": [], "history": []}
            new["keys"] = [dict(e) for e in new.get("keys", [])] + [entry]
            previous = new.get("current") if doc else external_current
            new["current"] = entry["kid"]
            for e in new["keys"]:
                if e["kid"] == previous:
                    e["stage_since"] = now
            self._note(new, by, "rotate", kid=entry["kid"], previous=previous)
            result["new"], result["previous"] = entry["kid"], previous
            return new

        self._mutate(fn)
        return str(result["new"]), result["previous"]

    def promote(self, kid: str, by: str | None, *, external_ids: Iterable[str], action: str = "rollback") -> str | None:
        """Make an existing, not retired key primary again (rollback).  Returns the key that was primary."""
        known = {e["kid"] for e in (self.doc or {}).get("keys", [])} | set(external_ids)
        if kid not in known or kid in self.retired:
            raise NotFound(f"key {kid} is not available for {action}", details={"available": sorted(known - set(self.retired))})
        result: dict[str, str | None] = {}

        def fn(doc: dict[str, Any] | None) -> dict[str, Any]:
            now = now_iso()
            new = dict(doc) if doc else {"name": self.name, "keys": [], "retired": [], "history": []}
            if kid in set(new.get("retired", [])):
                raise NotFound(f"key {kid} was retired")
            previous = new.get("current")
            new["keys"] = [dict(e) for e in new.get("keys", [])]
            if previous == kid:
                raise Conflict(f"key {kid} is already the primary key")
            for e in new["keys"]:
                if e["kid"] == previous:
                    e["stage_since"] = now
                if e["kid"] == kid:
                    e["stage_since"] = now
            new["current"] = kid
            self._note(new, by, action, kid=kid, previous=previous)
            result["previous"] = previous
            return new

        self._mutate(fn)
        return result["previous"]

    def drop_old(self, by: str | None, *, external_ids: Iterable[str], keep: str | None = None) -> list[str]:
        """Remove every overlap ring key and retire the external ones (all except the primary).  Returns the key ids that were dropped."""
        dropped: list[str] = []
        ext = set(external_ids)

        def fn(doc: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal dropped
            if not doc:
                raise Conflict("no rotation has been started: there is no old key to drop", remediation="run `olcf security rotate ... --action start` first")
            new = dict(doc)
            cur = keep or new.get("current")
            dropped = [e["kid"] for e in new.get("keys", []) if e["kid"] != cur]
            new["keys"] = [dict(e) for e in new.get("keys", []) if e["kid"] == cur]
            retired = set(new.get("retired", []))
            fresh = sorted((ext - {cur}) - retired)
            new["retired"] = sorted(retired | set(fresh))
            dropped = dropped + fresh
            new["current"] = cur
            self._note(new, by, "drop-old", dropped=dropped)
            return new

        self._mutate(fn)
        return dropped

    def rewrap(self, by: str | None) -> int:
        """Re-wrap every ring key under the newest root key (after the root key was rotated).  Returns the number of keys re-wrapped."""
        count = 0
        newest = self.wrapper.wrapper_id()

        def fn(doc: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal count
            if not doc:
                raise Conflict("there is no key ring yet: nothing to re-wrap")
            new = dict(doc)
            new["keys"] = []
            count = 0
            for e in doc.get("keys", []):
                e = dict(e)
                if e["wrapped_by"] != newest:
                    material = self.wrapper.unwrap(e["wrapped"], e["wrapped_by"], _aad(self.name, e["kid"]))
                    e["wrapped"], e["wrapped_by"] = self.wrapper.wrap(material, _aad(self.name, e["kid"]))
                    count += 1
                new["keys"].append(e)
            self._note(new, by, "rewrap", count=count, root=newest)
            return new

        self._mutate(fn)
        return count
