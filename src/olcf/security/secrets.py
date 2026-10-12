# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Secret storage (spec section 23).

Secret *values* are never part of connection metadata, item definitions, exports, logs or
capsules; they are referenced by opaque ``sec_...`` ids and resolved just-in-time by authorised
services via :meth:`SecretStore.resolve`.

Providers
---------
``file``   Envelope encryption (AES-256-GCM, per-secret data key wrapped by a master key). The master
           key comes from ``OLCF_MASTER_KEY`` (base64), the OS keyring (optional extra) or a 0600 file.
``vault``  HashiCorp Vault KV v2 over HTTPS (token from ``OLCF_VAULT_TOKEN``).
Additional providers (Azure Key Vault, AWS Secrets Manager, Windows DPAPI) plug in through
:class:`SecretProvider`.
"""

from __future__ import annotations

import base64
import json
import os
import secrets as _secrets
import struct
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import func, select, update

from olcf.core.errors import BadRequest, NotFound, Unavailable
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, insert_row
from olcf.security.keyring import Ring, refresh_seconds, unwrap_with, wrap_with

_MAGIC = b"OLS1"


class MasterKey:
    """The key-encryption keys, addressed by ``key_id``: the *root* keys (``OLCF_MASTER_KEY``, OS key ring or ``master.key``; newest last) plus the keys of
    the database key ring (:mod:`olcf.security.keyring`) that an administrator rotated in.

    ``current_id`` is the key new secrets are sealed with (the ring's primary, else the newest root key); ``keys`` is everything that can still decrypt.
    Root keys always remain available for *wrapping* the ring and for encrypting ``master``-mode backups (``backup_id``): a restore therefore needs only
    the root key, never a ring key that lives inside the backup itself.
    """

    def __init__(self, directory: Path, *, use_keyring: bool = False, explicit_file: str | None = None) -> None:
        self.dir = directory
        self.use_keyring = use_keyring
        self.file = Path(explicit_file) if explicit_file else directory / "master.key"
        self.root_keys: dict[str, bytes] = {}
        self.root_source = "file"
        self._ring = Ring("master", self, self._kid)
        self._merged: dict[str, bytes] = {}
        self._current = ""
        self._built_for: tuple[int, int] = (-1, -1)
        self._root_gen = 0
        self._load()

    @staticmethod
    def _kid(key: bytes) -> str:
        import hashlib

        return hashlib.sha256(b"olcf-kid" + key).hexdigest()[:8]

    def _load(self) -> None:
        raw: list[bytes] = []
        env = os.environ.get("OLCF_MASTER_KEY")
        if env:
            self.root_source = "env"
            raw.extend(base64.b64decode(part) for part in env.replace(",", " ").split())  # several keys while the root key is being rotated: newest last
        elif self.use_keyring:
            self.root_source = "keyring"
            try:
                import keyring  # type: ignore[import-not-found]

                val = keyring.get_password("olcf", "master-key")
                if not val:
                    val = base64.b64encode(AESGCM.generate_key(256)).decode()
                    keyring.set_password("olcf", "master-key", val)
                raw.append(base64.b64decode(val))
            except Exception as exc:  # noqa: BLE001
                raise Unavailable("keyring requested but unavailable", remediation="pip install olcf[keyring] or disable security.use_keyring") from exc
        else:
            self.root_source = "file"
            self.file.parent.mkdir(parents=True, exist_ok=True)
            if not self.file.exists():
                fd = os.open(self.file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as fh:
                    fh.write(base64.b64encode(AESGCM.generate_key(256)))
            for line in self.file.read_bytes().split():
                raw.append(base64.b64decode(line))
        if not raw:
            raise Unavailable("no master key available")
        self.root_keys = {}
        for k in raw:
            if len(k) != 32:
                raise Unavailable("master key must be 32 bytes")
            self.root_keys[self._kid(k)] = k
        self._root_order = [self._kid(k) for k in raw]  # last = newest
        self._root_gen += 1

    # ------------------------------------------------------------------ the database ring
    @property
    def ring(self) -> Ring:
        return self._ring

    def attach(self, db: Any) -> None:
        """Bind the key ring to the database (first call wins) and load it."""
        self._ring.attach(db)

    def refresh(self, *, force: bool = False) -> bool:
        return self._ring.refresh(force=force)

    def _rebuild(self) -> None:
        stamp = (self._ring.gen, self._root_gen)
        if stamp == self._built_for:
            return
        retired = self._ring.retired
        merged = {kid: k for kid, k in self.root_keys.items() if kid not in retired}
        merged.update(self._ring.keys)
        cur = self._ring.current
        if cur not in merged:
            live = [kid for kid in self._root_order if kid in merged]
            cur = live[-1] if live else (next(iter(merged)) if merged else self._root_order[-1])
        self._merged, self._current, self._built_for = merged, cur, stamp

    @property
    def keys(self) -> dict[str, bytes]:
        """Every key that can still decrypt (ring keys + root keys that were not retired)."""
        self._ring.refresh()
        self._rebuild()
        return self._merged

    @property
    def current_id(self) -> str:
        """The key id new secrets are sealed with."""
        self._ring.refresh()
        self._rebuild()
        return self._current

    @property
    def backup_id(self) -> str:
        """Key id used for ``master``-mode backups: the newest *root* key (restorable with nothing but the operator-held root key)."""
        return self._root_order[-1]

    def key_for(self, key_id: str) -> bytes | None:
        """The key with this id; an unknown id triggers one (throttled) immediate ring re-read, since another node may have just rotated."""
        k = self.keys.get(key_id)
        if k is None and self._ring.refresh(force=True):
            k = self.keys.get(key_id)
        return k

    # ------------------------------------------------------------------ wrapping under the root key (used by the rings)
    def wrapper_id(self) -> str:
        return self._root_order[-1]

    def wrap(self, plaintext: bytes, aad: bytes) -> tuple[str, str]:
        kid = self._root_order[-1]
        return wrap_with(self.root_keys[kid], plaintext, aad), kid

    def unwrap(self, blob: str, wrapped_by: str, aad: bytes) -> bytes:
        key = self.root_keys.get(wrapped_by)
        if key is None:
            raise Unavailable(f"root master key {wrapped_by} is not available on this node")
        return unwrap_with(key, blob, aad)

    def view_for(self, db: Any) -> MasterKey:
        """A read-only copy of this key set whose ring is the one stored in ``db`` (a restored or scratch database): same root keys, that database's ring."""
        other = MasterKey.__new__(MasterKey)
        other.dir, other.use_keyring, other.file = self.dir, self.use_keyring, self.file
        other.root_keys, other.root_source, other._root_order, other._root_gen = dict(self.root_keys), self.root_source, list(self._root_order), self._root_gen
        other._ring = Ring("master", other, other._kid)
        other._merged, other._current, other._built_for = {}, "", (-1, -1)
        other._ring.attach(db)
        return other

    # ------------------------------------------------------------------ rotation primitives (the service in olcf.api.core.rotation orchestrates them)
    def external_entries(self) -> dict[str, dict[str, Any]]:
        return {kid: {"source": self.root_source} for kid in self.root_keys}

    def rotate_ring(self, by: str | None = None) -> tuple[str, str | None]:
        """Make a freshly generated key primary (the previous one stays decrypt-only).  Propagates to every node through the database."""
        return self._ring.rotate(by, external_current=self._current_for_rotation())

    def _current_for_rotation(self) -> str:
        self._ring.refresh()
        self._rebuild()
        return self._current

    def promote(self, kid: str, by: str | None = None) -> str | None:
        return self._ring.promote(kid, by, external_ids=[k for k in self.root_keys if k not in self._ring.retired])

    def drop_old_ring(self, by: str | None = None) -> list[str]:
        return self._ring.drop_old(by, external_ids=list(self.root_keys))

    def rewrap_ring(self, by: str | None = None) -> int:
        return self._ring.rewrap(by)

    def key_entries(self) -> list[dict[str, Any]]:
        self._ring.refresh()
        self._rebuild()
        return self._ring.entries(self.external_entries(), primary=self._current)

    def rotate(self) -> str:
        """Legacy single-node rotation: append a new key to ``master.key``.  Prefer the API (``POST /v1/admin/security/rotate/master-key``), which stores the
        key in the shared database ring so that every node picks it up."""
        if os.environ.get("OLCF_MASTER_KEY") or self.use_keyring:
            raise BadRequest("master key rotation must be performed in the external key store")
        new = AESGCM.generate_key(256)
        with self.file.open("ab") as fh:
            fh.write(b"\n" + base64.b64encode(new))
        self.root_keys[self._kid(new)] = new
        self._root_order.append(self._kid(new))
        self._root_gen += 1
        return self._kid(new)

    def drop_old(self) -> None:
        """Remove all but the current key from disk (call after ``reencrypt_all``)."""
        if os.environ.get("OLCF_MASTER_KEY") or self.use_keyring:
            return
        newest = self._root_order[-1]
        self.file.write_bytes(base64.b64encode(self.root_keys[newest]))
        self.root_keys = {newest: self.root_keys[newest]}
        self._root_order = [newest]
        self._root_gen += 1


def seal(master: MasterKey, plaintext: bytes) -> tuple[bytes, str]:
    dek = AESGCM.generate_key(256)
    kid = master.current_id
    n1, n2 = os.urandom(12), os.urandom(12)
    wrapped = AESGCM(master.keys[kid]).encrypt(n1, dek, kid.encode())
    body = AESGCM(dek).encrypt(n2, plaintext, kid.encode())
    blob = _MAGIC + struct.pack(">H", len(wrapped)) + n1 + wrapped + n2 + body
    return blob, kid


def unseal(master: MasterKey, blob: bytes, key_id: str) -> bytes:
    if blob[:4] != _MAGIC:
        raise Unavailable("unrecognised secret envelope")
    (wlen,) = struct.unpack(">H", blob[4:6])
    n1, rest = blob[6:18], blob[18:]
    wrapped, rest = rest[:wlen], rest[wlen:]
    n2, body = rest[:12], rest[12:]
    key = master.key_for(key_id)
    if key is None:
        raise Unavailable(f"master key {key_id} not available")
    dek = AESGCM(key).decrypt(n1, wrapped, key_id.encode())
    return AESGCM(dek).decrypt(n2, body, key_id.encode())


class SecretProvider(ABC):
    """Interface of a secret backend: store a payload, fetch it, delete it and report health."""
    name = "abstract"

    @abstractmethod
    def store(self, secret_id: str, name: str, payload: str) -> dict[str, Any]:
        """Persist ``payload``; return columns to merge into the ``secrets`` row."""

    @abstractmethod
    def fetch(self, row: dict[str, Any]) -> str: ...

    def delete(self, row: dict[str, Any]) -> None:  # noqa: B027 - optional hook
        return None

    def health(self) -> tuple[bool, str]:
        return True, "ok"


class FileSecretProvider(SecretProvider):
    """Secret backend that encrypts payloads with a local master key and keeps them in the database."""
    name = "file"

    def __init__(self, master: MasterKey) -> None:
        self.master = master

    def store(self, secret_id: str, name: str, payload: str) -> dict[str, Any]:
        blob, kid = seal(self.master, payload.encode())
        return {"ciphertext": blob, "key_id": kid, "external_ref": None}

    def fetch(self, row: dict[str, Any]) -> str:
        return unseal(self.master, row["ciphertext"], row["key_id"]).decode()

    def health(self) -> tuple[bool, str]:
        try:
            blob, kid = seal(self.master, b"probe")
            assert unseal(self.master, blob, kid) == b"probe"
            return True, f"master key {self.master.current_id}"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)


class VaultSecretProvider(SecretProvider):
    """HashiCorp Vault KV v2. Secret value is stored as ``{"value": <payload>}`` at ``<mount>/data/<prefix>/<id>``."""

    name = "vault"

    def __init__(self, addr: str, mount: str = "secret", prefix: str = "olcf", token: str | None = None,
                 namespace: str | None = None, verify: bool | str = True, timeout: float = 10.0) -> None:
        self.addr = addr.rstrip("/")
        self.mount, self.prefix = mount.strip("/"), prefix.strip("/")
        self.token = token or os.environ.get("OLCF_VAULT_TOKEN", "")
        self.namespace = namespace
        self.verify = verify
        self.timeout = timeout

    def _client(self) -> httpx.Client:
        headers = {"X-Vault-Token": self.token}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        return httpx.Client(base_url=self.addr, headers=headers, verify=self.verify, timeout=self.timeout)

    def _path(self, ref: str) -> str:
        return f"/v1/{self.mount}/data/{ref}"

    def store(self, secret_id: str, name: str, payload: str) -> dict[str, Any]:
        ref = f"{self.prefix}/{secret_id}"
        with self._client() as c:
            r = c.post(self._path(ref), json={"data": {"value": payload}})
        if r.status_code >= 300:
            raise Unavailable(f"vault write failed ({r.status_code})")
        return {"ciphertext": None, "key_id": None, "external_ref": ref}

    def fetch(self, row: dict[str, Any]) -> str:
        with self._client() as c:
            r = c.get(self._path(row["external_ref"]))
        if r.status_code == 404:
            raise NotFound("secret missing in vault")
        if r.status_code >= 300:
            raise Unavailable(f"vault read failed ({r.status_code})")
        return r.json()["data"]["data"]["value"]

    def delete(self, row: dict[str, Any]) -> None:
        with self._client() as c:
            c.delete(f"/v1/{self.mount}/metadata/{row['external_ref']}")

    def health(self) -> tuple[bool, str]:
        try:
            with self._client() as c:
                r = c.get("/v1/sys/health", params={"standbyok": "true"})
            return r.status_code < 300, f"vault http {r.status_code}"
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"


class SecretStore:
    """Stores secrets through a provider and returns references. Secret values are decrypted only when a caller resolves a reference."""
    def __init__(self, db: Database, providers: dict[str, SecretProvider], default: str = "file", master: MasterKey | None = None) -> None:
        self.db = db
        self.providers = providers
        self.default = default
        self.master = master
        if master is not None:
            master.attach(db)  # the shared key ring: a rotation done on any node reaches this one within the refresh interval

    def _provider(self, name: str | None) -> SecretProvider:
        p = self.providers.get(name or self.default)
        if p is None:
            raise BadRequest(f"unknown secrets provider {name!r}", details={"available": sorted(self.providers)})
        return p

    @staticmethod
    def public(row: dict[str, Any]) -> dict[str, Any]:
        """Row projection that is safe to return from the API (never ciphertext)."""
        keep = ("id", "name", "provider", "key_id", "meta", "version", "scope_type", "scope_id", "created_at", "created_by", "rotated_at")
        return {k: row.get(k) for k in keep}

    def create(self, name: str, value: str | dict[str, Any], *, provider: str | None = None, scope_type: str = "org",
               scope_id: str = "", created_by: str | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = value if isinstance(value, str) else json.dumps(value)
        prov = self._provider(provider)
        sid = new_id("sec")
        cols = prov.store(sid, name, payload)
        meta = dict(meta or {})
        meta.setdefault("structured", not isinstance(value, str))
        row = {
            "id": sid, "name": name, "provider": prov.name, "meta": meta, "version": 1, "scope_type": scope_type,
            "scope_id": scope_id, "created_at": now_iso(), "created_by": created_by, "rotated_at": None, "deleted_at": None,
            "ciphertext": None, "external_ref": None, "key_id": None, **cols,
        }
        with self.db.begin() as conn:
            insert_row(conn, S.secrets, row)
        return self.public(row)

    def _row(self, secret_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = fetch_one(conn, select(S.secrets).where(S.secrets.c.id == secret_id, S.secrets.c.deleted_at.is_(None)))
        if not row:
            raise NotFound(f"secret {secret_id} not found")
        return row

    def get_meta(self, secret_id: str) -> dict[str, Any]:
        return self.public(self._row(secret_id))

    def resolve(self, secret_id: str) -> str | dict[str, Any]:
        """Return the secret value (``dict`` for structured secrets). Trusted-service use only."""
        row = self._row(secret_id)
        value = self._provider(row["provider"]).fetch(row)
        if row["meta"].get("structured"):
            return json.loads(value)
        return value

    def rotate(self, secret_id: str, new_value: str | dict[str, Any]) -> dict[str, Any]:
        row = self._row(secret_id)
        payload = new_value if isinstance(new_value, str) else json.dumps(new_value)
        cols = self._provider(row["provider"]).store(secret_id, row["name"], payload)
        meta = {**row["meta"], "structured": not isinstance(new_value, str)}
        with self.db.begin() as conn:
            conn.execute(update(S.secrets).where(S.secrets.c.id == secret_id).values(
                **cols, version=row["version"] + 1, rotated_at=now_iso(), meta=meta))
        return self.get_meta(secret_id)

    def delete(self, secret_id: str) -> None:
        row = self._row(secret_id)
        self._provider(row["provider"]).delete(row)
        with self.db.begin() as conn:
            conn.execute(update(S.secrets).where(S.secrets.c.id == secret_id).values(deleted_at=now_iso(), ciphertext=None))

    def list(self, *, scope_type: str | None = None, scope_id: str | None = None) -> list[dict[str, Any]]:
        stmt = select(S.secrets).where(S.secrets.c.deleted_at.is_(None)).order_by(S.secrets.c.id)
        if scope_type:
            stmt = stmt.where(S.secrets.c.scope_type == scope_type)
        if scope_id is not None:
            stmt = stmt.where(S.secrets.c.scope_id == scope_id)
        with self.db.connect() as conn:
            return [self.public(r) for r in fetch_all(conn, stmt)]

    def key_usage(self) -> dict[str, int]:
        """How many live secrets are sealed under each master-key id (``"-"`` counts secrets held by an external provider such as Vault)."""
        out: dict[str, int] = {}
        with self.db.connect() as conn:
            for r in conn.execute(select(S.secrets.c.provider, S.secrets.c.key_id).where(S.secrets.c.deleted_at.is_(None))).all():
                kid = str(r[1]) if r[0] == "file" and r[1] else "-"
                out[kid] = out.get(kid, 0) + 1
        return out

    def pending_reencryption(self) -> int:
        """Live ``file`` secrets that are not sealed under the current master key."""
        if self.master is None:
            return 0
        cur = self.master.current_id
        with self.db.connect() as conn:
            return int(conn.execute(select(func.count()).select_from(S.secrets).where(
                S.secrets.c.provider == "file", S.secrets.c.deleted_at.is_(None), S.secrets.c.key_id != cur)).scalar() or 0)

    def reencrypt_all(self, *, batch: int = 100, should_stop: Callable[[], bool] | None = None,
                      on_progress: Callable[[int, int, int], None] | None = None) -> dict[str, Any]:
        """Re-seal every ``file`` secret under the current master key.  Resumable, idempotent and safe next to concurrent secret writes.

        Each row is rewritten with a compare-and-swap on ``(id, key_id, version)``: a secret that was rotated (new value) or deleted in the meantime is left
        alone, and a secret created under the *new* key is never touched.  Rows that cannot be decrypted (their key is not available here) are reported
        and skipped instead of aborting the pass.  Several passes run until nothing is left to do (a lagging node may still write under the old key until
        it has re-read the ring).  Returns ``{"reencrypted", "skipped", "failed": [ids], "remaining"}``.
        """
        if self.master is None:
            return {"reencrypted": 0, "skipped": 0, "failed": [], "remaining": 0}
        done = skipped = 0
        failed: dict[str, str] = {}
        t = S.secrets
        for _pass in range(4):
            cursor = ""
            progressed = False
            while True:
                if should_stop and should_stop():
                    return {"reencrypted": done, "skipped": skipped, "failed": sorted(failed), "remaining": self.pending_reencryption(), "stopped": True}
                cur = self.master.current_id
                with self.db.connect() as conn:
                    rows = fetch_all(conn, select(t).where(t.c.provider == "file", t.c.deleted_at.is_(None), t.c.key_id != cur, t.c.id > cursor).order_by(t.c.id).limit(batch))
                if not rows:
                    break
                for r in rows:
                    cursor = r["id"]
                    try:
                        blob, kid = seal(self.master, unseal(self.master, r["ciphertext"], r["key_id"]))
                    except Exception as exc:  # noqa: BLE001 - key missing / damaged envelope
                        failed[r["id"]] = f"{type(exc).__name__}: {exc}"[:200]
                        continue
                    with self.db.begin() as conn:
                        n = conn.execute(update(t).where(t.c.id == r["id"], t.c.key_id == r["key_id"], t.c.version == r["version"], t.c.deleted_at.is_(None))
                                         .values(ciphertext=blob, key_id=kid)).rowcount
                    if n:
                        done += 1
                        progressed = True
                    else:
                        skipped += 1
                if on_progress:
                    on_progress(done, skipped, len(failed))
            remaining = self.pending_reencryption()
            if remaining - len(failed) <= 0 or not progressed:
                break
            time.sleep(min(2.0, refresh_seconds()))
        return {"reencrypted": done, "skipped": skipped, "failed": sorted(failed), "failures": failed, "remaining": self.pending_reencryption()}

    def health(self) -> tuple[bool, str]:
        return self._provider(None).health()


def random_secret(nbytes: int = 24) -> str:
    return base64.urlsafe_b64encode(_secrets.token_bytes(nbytes)).decode().rstrip("=")
