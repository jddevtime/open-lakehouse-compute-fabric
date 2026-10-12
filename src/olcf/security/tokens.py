# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Platform-issued credentials.

* **Access tokens** - short-lived HS256 JWTs (``iss=olcf``) for interactive users.
* **Refresh tokens** - opaque, rotating, stored hashed (server-side refresh, spec 31 "Tokens").
* **Personal / service access tokens** (``olcf_pat_<id>_<secret>``) - for SDK, CLI, MCP agents,
  Fabric notebooks. Only a SHA-256 of the secret is stored; the plaintext is shown once.
* **Enrollment tokens** (``olcf_enr_...``) - single-use, short-lived, used by worker agents.
* **Worker tokens** (``olcf_wrk_...``) - long-lived worker channel credential, rotatable.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets as _secrets
import threading
import time
from pathlib import Path
from typing import Any

import jwt
from sqlalchemy import select, update

from olcf.core.errors import BadRequest, Unauthenticated, Unavailable
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_one, insert_row
from olcf.security.keyring import Ring

ISSUER = "olcf"
AUDIENCE = "olcf-api"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class SigningKeys:
    """HS256 signing keys.

    The *external* keys come from ``OLCF_JWT_SIGNING_KEY`` or the 0600 files ``jwt-signing.key`` / ``jwt-signing.prev.key`` below the secrets directory.
    On top of them sits the **database key ring** (:mod:`olcf.security.keyring`), which an administrator rotates through the API: its primary key
    signs new tokens, older keys keep verifying (overlap) until they are dropped, and every control-plane node adopts a rotation within the ring refresh
    interval.  Tokens carry the ``kid`` of their key, so verification picks the right key directly; tokens without one (older ones) try every key.
    """

    def __init__(self, directory: Path, master: Any = None) -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.source = "env" if os.environ.get("OLCF_JWT_SIGNING_KEY") else "file"
        self._file_current = self._load("jwt-signing.key", create=True)
        self._file_previous = self._load("jwt-signing.prev.key", create=False)
        self._gen = 0
        self._built_for: tuple[int, int] = (-1, -1)
        self._order: list[tuple[str, bytes]] = []
        self.master = master
        self.ring: Ring | None = Ring("jwt", master, self.kid_of) if master is not None else None

    @staticmethod
    def kid_of(key: bytes) -> str:
        return hashlib.sha256(b"olcf-jwt-kid" + key).hexdigest()[:8]

    def _load(self, name: str, *, create: bool) -> bytes | None:
        env = os.environ.get("OLCF_JWT_SIGNING_KEY") if name == "jwt-signing.key" else None
        if env:
            return env.encode()
        p = self.dir / name
        if p.exists():
            return p.read_bytes().strip()
        if not create:
            return None
        key = base64.urlsafe_b64encode(os.urandom(48))
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(key)
        return key

    # ------------------------------------------------------------------ resolved view (ring + external)
    def attach(self, db: Database) -> None:
        if self.ring is not None:
            self.ring.attach(db)

    def refresh(self, *, force: bool = False) -> bool:
        return self.ring.refresh(force=force) if self.ring is not None else False

    def _external(self) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for k in (self._file_current, self._file_previous):
            if k:
                out.setdefault(self.kid_of(k), k)
        return out

    def _resolved(self) -> list[tuple[str, bytes]]:
        """``[(kid, key)]`` - the signing key first, then every other key that still verifies."""
        if self.ring is not None:
            self.ring.refresh()
        stamp = (self.ring.gen if self.ring is not None else 0, self._gen)
        if stamp == self._built_for:
            return self._order
        ext = self._external()
        retired = self.ring.retired if self.ring is not None else frozenset()
        ring_keys = dict(self.ring.keys) if self.ring is not None else {}
        usable: dict[str, bytes] = {kid: k for kid, k in ext.items() if kid not in retired}
        usable.update(ring_keys)
        ext_current = self.kid_of(self._file_current) if self._file_current else ""
        cur = (self.ring.current if self.ring is not None else None) or ext_current
        if cur not in usable:
            cur = ext_current if ext_current in usable else next(iter(usable))
        stage_since = {e["kid"]: e.get("stage_since") or "" for e in (self.ring.doc or {}).get("keys", [])} if self.ring is not None and self.ring.doc else {}
        rest = sorted((kid for kid in usable if kid != cur), key=lambda kid: (kid in ring_keys, stage_since.get(kid, "")), reverse=True)
        self._order = [(cur, usable[cur]), *((kid, usable[kid]) for kid in rest)]
        self._built_for = stamp
        return self._order

    @property
    def current(self) -> bytes:
        return self._resolved()[0][1]

    @property
    def current_kid(self) -> str:
        return self._resolved()[0][0]

    @property
    def previous(self) -> bytes | None:
        """The key that was primary before (kept for callers that only know two keys); ``None`` when there is no other key."""
        order = self._resolved()
        return order[1][1] if len(order) > 1 else None

    def verification_keys(self) -> list[tuple[str, bytes]]:
        return list(self._resolved())

    def key_for(self, kid: str) -> bytes | None:
        """The verification key with this id; an unknown id triggers one (throttled) immediate ring re-read, since another node may just have rotated."""
        for k, key in self._resolved():
            if k == kid:
                return key
        if self.refresh(force=True):
            for k, key in self._resolved():
                if k == kid:
                    return key
        return None

    # ------------------------------------------------------------------ rotation
    def rotate(self) -> None:
        """Legacy single-node rotation of the key *files* (the old key moves to ``jwt-signing.prev.key``).  The API rotates the shared database ring instead
        (``POST /v1/admin/security/rotate/jwt-signing-key``), which every node follows."""
        if self.ring is not None and self.ring.doc:
            raise BadRequest("the signing keys are managed through the key ring; use `olcf security rotate jwt-signing-key`")
        p, prev = self.dir / "jwt-signing.key", self.dir / "jwt-signing.prev.key"
        if p.exists():
            prev.write_bytes(p.read_bytes())
            os.chmod(prev, 0o600)
            p.unlink()
        self._file_current = self._load("jwt-signing.key", create=True)
        self._file_previous = self._load("jwt-signing.prev.key", create=False)
        self._gen += 1

    def rotate_ring(self, by: str | None = None) -> tuple[str, str | None]:
        """Make a freshly generated key the signing key; the previous one keeps verifying until it is dropped."""
        if self.ring is None:
            raise Unavailable("signing-key rotation needs the master key to wrap the key ring")
        self._resolved()
        return self.ring.rotate(by, external_current=self.current_kid, material=base64.urlsafe_b64encode(os.urandom(48)))

    def promote(self, kid: str, by: str | None = None) -> str | None:
        if self.ring is None:
            raise Unavailable("signing-key rotation needs the master key to wrap the key ring")
        return self.ring.promote(kid, by, external_ids=[k for k in self._external() if k not in self.ring.retired])

    def drop_old_ring(self, by: str | None = None) -> list[str]:
        if self.ring is None:
            raise Unavailable("signing-key rotation needs the master key to wrap the key ring")
        return self.ring.drop_old(by, external_ids=list(self._external()), keep=self.current_kid)

    def key_entries(self) -> list[dict[str, Any]]:
        order = self._resolved()
        ext = {kid: {"source": self.source if kid == self.kid_of(self._file_current or b"") else "file"} for kid in self._external()}
        if self.ring is not None:
            return self.ring.entries(ext, primary=order[0][0])
        return [{"kid": kid, "stage": "primary" if i == 0 else "overlap", "source": ext.get(kid, {}).get("source", "file"), "externally_managed": True, "available": True}
                for i, (kid, _key) in enumerate(order)]


class TokenService:
    """Issues, verifies, refreshes and revokes access and refresh tokens, and keeps a short-lived cache of the revoked list."""
    #: How long this node trusts its copy of the revoked-token list. A revocation written by another node is honoured here within this many seconds
    #: (like the Authorizer's binding cache); a revocation made on this node applies immediately.
    REVOCATION_CACHE_SECONDS = 2.0
    #: A rotated refresh token presented again within this window is treated as a client race (two tabs refreshing at once), not as theft.
    REUSE_GRACE_SECONDS = 10.0

    def __init__(self, db: Database, keys: SigningKeys, *, access_ttl: int = 1800, refresh_ttl: int = 14 * 86400) -> None:
        self.db = db
        self.keys = keys
        self.access_ttl = access_ttl
        self.refresh_ttl = refresh_ttl
        self._revoked: set[str] = set()
        self._revoked_loaded_at = float("-inf")
        self._revoked_lock = threading.Lock()
        keys.attach(db)  # joins the shared key ring (staged JWT-key rotation reaches this node within the refresh interval)

    # ------------------------------------------------------------------ JWT access tokens
    def issue_access_token(self, principal: dict[str, Any], *, scopes: list[str] | None = None, ttl: int | None = None, family: str | None = None) -> tuple[str, int]:
        """Mint an access token. ``family`` is the id of the login session (shared with its refresh tokens): revoking the family ends every access token of it."""
        now = int(time.time())
        ttl = ttl or self.access_ttl
        payload = {
            "iss": ISSUER, "aud": AUDIENCE, "sub": principal["id"], "name": principal["name"], "kind": principal["kind"],
            "iat": now, "nbf": now - 5, "exp": now + ttl, "jti": _secrets.token_hex(8), "scp": scopes or ["*"],
        }
        if family:
            payload["fam"] = family
        kid, key = self.keys.verification_keys()[0]
        return jwt.encode(payload, key, algorithm="HS256", headers={"kid": kid}), ttl

    def verify_access_token(self, token: str) -> dict[str, Any]:
        last_exc: Exception | None = None
        try:
            header_kid = jwt.get_unverified_header(token).get("kid")
        except jwt.InvalidTokenError as exc:
            raise Unauthenticated("invalid access token") from exc
        if header_kid and isinstance(header_kid, str):
            known = self.keys.key_for(header_kid)  # a key id announced by a rotation on another node triggers one ring re-read
            candidates: list[bytes] = [known] if known else []
        else:
            candidates = [k for _kid, k in self.keys.verification_keys()]
        for key in candidates:
            try:
                claims = jwt.decode(token, key, algorithms=["HS256"], audience=AUDIENCE, issuer=ISSUER, leeway=10,
                                    options={"require": ["exp", "sub", "iss", "aud"]})
                if self.is_revoked(claims.get("jti"), claims.get("fam")):
                    raise Unauthenticated("access token revoked", remediation="sign in again")
                return claims
            except jwt.ExpiredSignatureError as exc:
                raise Unauthenticated("access token expired") from exc
            except jwt.InvalidTokenError as exc:
                last_exc = exc
        raise Unauthenticated("invalid access token") from last_exc

    # ------------------------------------------------------------------ revocation of access tokens
    def _load_revoked(self, *, force: bool = False) -> set[str]:
        now = time.monotonic()
        with self._revoked_lock:
            if not force and now - self._revoked_loaded_at < self.REVOCATION_CACHE_SECONDS:
                return self._revoked
        try:
            with self.db.connect() as conn:
                ids = {r[0] for r in conn.execute(select(S.revoked_tokens.c.jti).where(S.revoked_tokens.c.expires_at > now_iso())).all()}
        except Exception:  # noqa: BLE001 - table missing (not yet migrated) or database hiccup: keep the last known list, local revocations still apply
            with self._revoked_lock:
                self._revoked_loaded_at = now
                return self._revoked
        with self._revoked_lock:
            self._revoked = ids
            self._revoked_loaded_at = now
            return self._revoked

    def is_revoked(self, jti: str | None, family: str | None = None) -> bool:
        """True when this access token (or the login session it belongs to) was revoked by a logout or by refresh-token reuse detection."""
        revoked = self._load_revoked()
        return bool((jti and jti in revoked) or (family and f"fam:{family}" in revoked))

    def revoke_access(self, jti: str, *, principal_id: str | None, expires_at: float | None = None, reason: str = "logout") -> None:
        """Revoke one access token (``jti``) or a whole login session (``fam:<id>``) until it would have expired anyway."""
        exp = expires_at if expires_at else time.time() + self.access_ttl + 30
        row = {"jti": jti, "principal_id": principal_id, "reason": reason, "revoked_at": now_iso(), "expires_at": iso_in(int(exp - time.time()) + 15)}
        with self.db.begin() as conn:
            if conn.execute(update(S.revoked_tokens).where(S.revoked_tokens.c.jti == jti).values(**{k: v for k, v in row.items() if k != "jti"})).rowcount == 0:
                try:
                    with conn.begin_nested():
                        conn.execute(S.revoked_tokens.insert().values(**row))
                except Exception:  # noqa: BLE001 - concurrent insert of the same jti: already revoked
                    pass
        with self._revoked_lock:  # applies at once on this node; the next check re-reads the table (which now contains the row) for the other nodes' changes
            self._revoked = set(self._revoked) | {jti}
            self._revoked_loaded_at = float("-inf")

    def revoke_family(self, family: str, principal_id: str, *, reason: str = "refresh_token_reuse") -> int:
        """End a whole login session: every refresh token of the family is revoked and every access token minted for it stops working. Returns the refresh tokens revoked."""
        self.revoke_access(f"fam:{family}", principal_id=principal_id, reason=reason, expires_at=time.time() + self.access_ttl + 30)
        n = 0
        t = S.api_tokens
        with self.db.connect() as conn:
            rows = conn.execute(select(t.c.id, t.c.meta).where(t.c.principal_id == principal_id, t.c.kind == "refresh", t.c.revoked_at.is_(None))).all()
        for tid, meta in rows:
            if (meta or {}).get("family") == family or tid == family:
                n += int(self.revoke(tid))
        return n

    @staticmethod
    def looks_like_local_jwt(token: str) -> bool:
        try:
            return jwt.decode(token, options={"verify_signature": False}).get("iss") == ISSUER
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------ opaque tokens
    def _create_opaque(self, principal_id: str, kind: str, label: str, *, name: str | None, scopes: list[str] | None,
                       ttl: int | None, meta: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        tid = new_id("tok")
        secret = base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("=")
        handle = tid.split("_", 1)[1][-12:].lower()
        plaintext = f"olcf_{label}_{handle}_{secret}"
        rec = {
            "id": tid, "principal_id": principal_id, "kind": kind, "name": name, "prefix": f"olcf_{label}_{handle}",
            "token_hash": _sha256(secret), "scopes": scopes or ["*"], "meta": meta or {}, "created_at": now_iso(),
            "expires_at": iso_in(ttl) if ttl else None, "last_used_at": None, "revoked_at": None,
        }
        with self.db.begin() as conn:
            insert_row(conn, S.api_tokens, rec)
        return plaintext, rec

    def create_pat(self, principal_id: str, name: str, *, scopes: list[str] | None = None, ttl_days: int | None = 90,
                   meta: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        return self._create_opaque(principal_id, "pat", "pat", name=name, scopes=scopes, ttl=ttl_days * 86400 if ttl_days else None, meta=meta)

    def create_derived(self, principal_id: str, credential: dict[str, Any], *, name: str, ttl: int, binding: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Mint a credential *derived* from another one (a job's or kernel's callback token).

        It carries only the minimum ``job`` scope; ``credential`` is the persisted ceiling document of the submitting credential
        (:meth:`olcf.security.rbac.Principal.credential_ceiling`) and ``binding`` names what the credential is bound to (``job_id`` + ``attempt`` or ``session_id``).
        ``olcf.api.deps.authenticate`` turns both into the effective permissions - scopes ∩ ceiling ∩ the owner's *current* role bindings - locked to one workspace, and
        refuses the credential once its origin credential, its attempt, its job or its session ended.
        """
        return self._create_opaque(principal_id, "pat", "pat", name=name, scopes=["job"], ttl=ttl, meta={**binding, "derived": dict(credential)})

    def derived_origin_revoked(self, origin_token_id: str | None, origin_family: str | None) -> bool:
        """True when the user credential at the root of a derivation chain was revoked (PAT revoked or expired-and-revoked, access token logged out, login session ended)."""
        if origin_family and self.is_revoked(None, origin_family):
            return True
        if not origin_token_id:
            return False
        if origin_token_id.startswith("tok_"):
            with self.db.connect() as conn:
                row = fetch_one(conn, select(S.api_tokens.c.revoked_at).where(S.api_tokens.c.id == origin_token_id))
            return row is None or bool(row["revoked_at"])
        return self.is_revoked(origin_token_id, None)  # a login access token: its jti

    def create_refresh(self, principal_id: str, meta: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        return self._create_opaque(principal_id, "refresh", "ref", name="refresh", scopes=["*"], ttl=self.refresh_ttl, meta=meta)

    def create_enrollment(self, principal_id: str, *, ttl_seconds: int = 3600, meta: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        return self._create_opaque(principal_id, "enroll", "enr", name="worker enrollment", scopes=["worker"], ttl=ttl_seconds, meta=meta)

    def create_worker_token(self, principal_id: str, *, meta: dict[str, Any] | None = None, ttl_seconds: int | None = None) -> tuple[str, dict[str, Any]]:
        """The long-lived credential of a worker's control channel; ``ttl_seconds`` (the max-age policy) makes it expire, ``None`` keeps it unlimited."""
        return self._create_opaque(principal_id, "worker", "wrk", name="worker channel", scopes=["worker"], ttl=ttl_seconds, meta=meta)

    def verify_opaque(self, token: str, *, kinds: tuple[str, ...] = ("pat",), touch: bool = True) -> dict[str, Any]:
        """Validate an opaque token and return its row. Raises :class:`Unauthenticated`."""
        parts = token.split("_", 3)
        if len(parts) != 4 or parts[0] != "olcf":
            raise Unauthenticated("malformed token")
        prefix = "_".join(parts[:3])
        secret = parts[3]
        with self.db.connect() as conn:
            row = fetch_one(conn, select(S.api_tokens).where(S.api_tokens.c.prefix == prefix))
        if not row or not hmac.compare_digest(row["token_hash"], _sha256(secret)):
            raise Unauthenticated("invalid token")
        if row["kind"] not in kinds:
            raise Unauthenticated("token type not accepted here")
        if row["revoked_at"]:
            raise Unauthenticated("token revoked")
        if row["expires_at"] and from_iso(row["expires_at"]).timestamp() < time.time():
            raise Unauthenticated("token expired")
        if touch:
            try:
                with self.db.begin() as conn:
                    conn.execute(update(S.api_tokens).where(S.api_tokens.c.id == row["id"]).values(last_used_at=now_iso()))
            except Exception:  # noqa: BLE001 - never fail auth because of a bookkeeping write
                pass
        return row

    def inspect_opaque(self, token: str, *, kinds: tuple[str, ...] = ("refresh",)) -> dict[str, Any] | None:
        """The row behind an opaque token whose secret is correct, **whether or not it is still valid** (used to recognise a replayed, already-rotated refresh token)."""
        parts = token.split("_", 3)
        if len(parts) != 4 or parts[0] != "olcf":
            return None
        with self.db.connect() as conn:
            row = fetch_one(conn, select(S.api_tokens).where(S.api_tokens.c.prefix == "_".join(parts[:3])))
        if not row or row["kind"] not in kinds or not hmac.compare_digest(row["token_hash"], _sha256(parts[3])):
            return None
        return row

    def consume_enrollment(self, token: str) -> dict[str, Any]:
        row = self.verify_opaque(token, kinds=("enroll",), touch=False)
        self.revoke(row["id"])  # single use
        return row

    def revoke(self, token_id: str) -> bool:
        with self.db.begin() as conn:
            return conn.execute(update(S.api_tokens).where(S.api_tokens.c.id == token_id, S.api_tokens.c.revoked_at.is_(None))
                                .values(revoked_at=now_iso())).rowcount > 0

    def revoke_all_for(self, principal_id: str, kinds: tuple[str, ...] | None = None) -> int:
        stmt = update(S.api_tokens).where(S.api_tokens.c.principal_id == principal_id, S.api_tokens.c.revoked_at.is_(None))
        if kinds:
            stmt = stmt.where(S.api_tokens.c.kind.in_(kinds))
        with self.db.begin() as conn:
            return conn.execute(stmt.values(revoked_at=now_iso())).rowcount
