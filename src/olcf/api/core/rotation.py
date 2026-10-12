# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Credential lifecycle: staged rotation of the master key and the JWT signing key (permission ``admin.keys``, role SecurityAdmin).

``GET  /v1/admin/security/keys``                         key ids, stages, ages, sources and usage - never key material
``POST /v1/admin/security/rotate/master-key``            ``action`` = ``start`` (default) | ``reencrypt`` | ``drop-old`` | ``rollback`` | ``rewrap``
``POST /v1/admin/security/rotate/jwt-signing-key``       ``action`` = ``start`` | ``drop-old`` | ``rollback`` | ``rewrap``

Staged overlap
--------------
``start`` generates a key into the shared database key ring (:mod:`olcf.security.keyring`) and makes it primary.  The previous primary stays as an
*overlap* key: it still decrypts secrets / verifies tokens, nothing new uses it.  Every control-plane node adopts the new primary within the ring
refresh interval (``OLCF_KEYRING_REFRESH_SECONDS``, default 5 s) - no restart, no file copying.  Keys supplied from the environment, the OS key ring or
``master.key`` keep working and are reported as externally managed (they cannot be generated through the API; they are retired by ``drop-old``).

* master key: ``start`` also launches the resumable **re-encryption** operation (``reencrypt``): every ``file`` secret is re-sealed under the primary
  key, one compare-and-swap at a time, so it is safe next to concurrent secret writes and can be re-run after a crash.  ``drop-old`` is refused until
  no secret is sealed under an older key.
* JWT key: nothing to re-encrypt; ``drop-old`` is refused until the previous key is older than the longest access-token lifetime (``force`` overrides
  and signs users out).
* ``rollback`` makes the previous key primary again (before ``drop-old``); ``rewrap`` re-wraps the ring under a new *root* key after the operator
  rotated ``OLCF_MASTER_KEY`` (new key appended: ``OLCF_MASTER_KEY="<old> <new>"``).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from olcf.api.deps import Authed, Ctx
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.timeutil import from_iso, now_iso
from olcf.db import schema as S
from olcf.ops.common import run_operation
from olcf.security.keyring import refresh_seconds

router = APIRouter(tags=["security"])

KINDS = ("master-key", "jwt-signing-key")
LOCK_STALE_SECONDS = 60.0
#: an overlap key that is still around after this many days means a rotation was started and never finished
STUCK_AFTER_DAYS = 7


class RotateBody(BaseModel):
    """Body of ``POST /admin/security/rotate/{kind}``: the stage to run and its options."""
    model_config = ConfigDict(extra="forbid")
    action: Literal["start", "reencrypt", "drop-old", "rollback", "rewrap"] = "start"
    reencrypt: bool = Field(True, description="master key ``start`` / ``rollback``: also launch the re-encryption operation")
    force: bool = Field(False, description="jwt ``drop-old``: drop before the previous key is older than the longest token lifetime (signs users out)")
    to: str | None = Field(None, max_length=16, description="``rollback``: key id to make primary again (default: the previous primary)")


def _age_days(iso: str | None) -> float | None:
    if not iso:
        return None
    return round((datetime.now(UTC) - from_iso(iso)).total_seconds() / 86400, 3)


class KeyRotationService:
    """Orchestrates the key rings, the re-encryption operation and the audit trail.  One instance per node (state lives in the database)."""

    LOCK_KEY = "security.reencrypt.master"  # deliberately outside the ring prefix

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ helpers
    @property
    def master(self):  # noqa: ANN201
        return self.ctx.secrets.master

    @property
    def jwt(self):  # noqa: ANN201
        return self.ctx.tokens.keys

    def _ring_of(self, kind: str):  # noqa: ANN202
        if kind == "master-key":
            return self.master
        if kind == "jwt-signing-key":
            return self.jwt
        raise NotFound(f"unknown key kind {kind!r}", details={"kinds": list(KINDS)})

    def max_token_lifetime(self) -> int:
        return int(self.ctx.tokens.access_ttl) + 30  # access ttl + the 10 s verification leeway + clock slack

    def _audit(self, principal, action: str, kind: str, outcome: str = "success", **details: Any) -> None:  # noqa: ANN001
        self.ctx.audit.record(principal, f"security.{action}", resource_type="key_ring", resource_id=kind, outcome=outcome, details=details)
        if outcome == "success":
            self.ctx.events.publish(f"security.{action}", {"kind": kind, **details})

    # ------------------------------------------------------------------ status
    def reencryption_state(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            row = conn.execute(select(S.kv.c.value, S.kv.c.updated_at).where(S.kv.c.key == self.LOCK_KEY)).first()
        if not row:
            return None
        doc = dict(row[0])
        doc["heartbeat_age_seconds"] = round((datetime.now(UTC) - from_iso(row[1])).total_seconds(), 1)
        doc["stale"] = doc.get("state") == "running" and doc["heartbeat_age_seconds"] > LOCK_STALE_SECONDS
        return doc

    def status(self) -> dict[str, Any]:
        self.master.refresh()
        self.jwt.refresh()
        usage = self.ctx.secrets.key_usage()
        mk_entries = self.master.key_entries()
        for e in mk_entries:
            e["secrets"] = usage.get(e["kid"], 0)
            e["age_days"] = _age_days(e.get("created_at"))
        primary = next((e["kid"] for e in mk_entries if e["stage"] == "primary"), self.master.current_id)
        pending = sum(n for kid, n in usage.items() if kid not in ("-", primary))
        mring = self.master.ring
        blockers_m = self._master_drop_blockers(usage, primary)
        mk = {
            "kind": "master-key", "primary": primary, "root_source": self.master.root_source, "root_key": self.master.backup_id,
            "externally_managed": self.master.root_source != "file" or not mring.doc,
            "note": "the root key comes from the environment / OS key ring and wraps the ring; rotate it by adding the new key to OLCF_MASTER_KEY and running `rewrap`"
                    if self.master.root_source != "file" else "root key is the local master.key file",
            "ring_version": mring.version, "keys": mk_entries,
            "secrets": {"total": sum(usage.values()), "by_key": usage, "pending_reencryption": pending, "external_provider": usage.get("-", 0)},
            "reencryption": self.reencryption_state(), "can_drop_old": not blockers_m and any(e["stage"] != "primary" for e in mk_entries if e["stage"] != "retired"),
            "drop_old_blockers": blockers_m, "history": mring.history()[-10:], "errors": list(mring.errors),
        }
        jr = self.jwt.ring
        jk_entries = self.jwt.key_entries()
        for e in jk_entries:
            e["age_days"] = _age_days(e.get("created_at"))
        blockers_j, eligible_at = self._jwt_drop_blockers(jk_entries)
        jw = {
            "kind": "jwt-signing-key", "primary": next((e["kid"] for e in jk_entries if e["stage"] == "primary"), self.jwt.current_kid),
            "root_source": self.jwt.source, "externally_managed": self.jwt.source == "env" or not (jr and jr.doc),
            "ring_version": jr.version if jr else 0, "keys": jk_entries, "max_token_lifetime_seconds": self.max_token_lifetime(),
            "drop_old_eligible_at": eligible_at, "can_drop_old": not blockers_j and any(e["stage"] == "overlap" for e in jk_entries),
            "drop_old_blockers": blockers_j, "history": jr.history()[-10:] if jr else [], "errors": list(jr.errors) if jr else [],
        }
        return {"refresh_interval_seconds": refresh_seconds(), "node": self.ctx.node_id, "master_key": mk, "jwt_signing_key": jw, "checked_at": now_iso()}

    def _settle_wait(self, ring) -> float:  # noqa: ANN001
        """Seconds until every healthy node has had time to adopt the latest rotation / rollback."""
        last = next((h for h in reversed(ring.history()) if h.get("action") in ("rotate", "rollback")), None)
        if not last:
            return 0.0
        elapsed = (datetime.now(UTC) - from_iso(last["at"])).total_seconds()
        return max(0.0, refresh_seconds() + 2 - elapsed) if refresh_seconds() > 0 else 0.0

    def _master_drop_blockers(self, usage: dict[str, int], primary: str) -> list[str]:
        out = []
        stale = {k: n for k, n in usage.items() if k not in ("-", primary)}
        if stale:
            out.append(f"{sum(stale.values())} secret(s) are still sealed under older key(s) {sorted(stale)}: run the re-encryption first")
        wait = self._settle_wait(self.master.ring)
        if wait > 0:
            out.append(f"the latest rotation is only {int(refresh_seconds() + 2 - wait)} s old: wait {int(wait) + 1} s so every node has adopted the new primary key")
        st = self.reencryption_state()
        if st and st.get("state") == "running" and not st.get("stale"):
            out.append("a re-encryption is still running")
        return out

    def _jwt_drop_blockers(self, entries: list[dict[str, Any]]) -> tuple[list[str], str | None]:
        out: list[str] = []
        eligible_at: str | None = None
        now = datetime.now(UTC)
        newest_demotion = None
        for e in entries:
            if e["stage"] == "overlap" and e.get("source") == "ring" and e.get("stage_since"):
                t = from_iso(e["stage_since"])
                newest_demotion = max(newest_demotion, t) if newest_demotion else t
        # a key that left the primary role is dangerous until every token it signed has expired; the rotation moment bounds that for the most recent one
        last = next((h for h in reversed(self.jwt.ring.history()) if h.get("action") in ("rotate", "rollback")), None) if self.jwt.ring else None
        if last:
            t = from_iso(last["at"])
            newest_demotion = max(newest_demotion, t) if newest_demotion else t
        if newest_demotion:
            from datetime import timedelta

            eligible = newest_demotion + timedelta(seconds=self.max_token_lifetime())
            eligible_at = eligible.isoformat().replace("+00:00", "Z")
            if eligible > now:
                out.append(f"tokens signed with the previous key can live until {eligible_at}; dropping it earlier signs those users out (force=true to do it anyway)")
        return out, eligible_at

    # ------------------------------------------------------------------ master key
    def rotate_master(self, principal, *, reencrypt: bool, wait: bool) -> dict[str, Any]:  # noqa: ANN001
        if self.master.ring.db is None:
            raise BadRequest("the key ring is not available on this node")
        new, previous = self.master.rotate_ring(principal.id)
        self._audit(principal, "master_key.rotated", "master-key", new_key=new, previous_key=previous)
        out: dict[str, Any] = {"kind": "master-key", "action": "start", "new_key": new, "previous_key": previous,
                               "propagation": f"every control-plane node adopts the new key within {refresh_seconds():g} s"}
        if reencrypt:
            out.update(self.reencrypt(principal, wait=wait))
        return out

    def _claim_lock(self, oid: str) -> None:
        """One re-encryption at a time across all nodes: a ``kv`` row with a heartbeat; a row whose heartbeat is stale is taken over (crash recovery)."""
        from sqlalchemy import update
        from sqlalchemy.exc import IntegrityError

        doc = {"state": "running", "operation_id": oid, "node": self.ctx.node_id, "started_at": now_iso(), "done": 0, "skipped": 0, "failed": 0, "total": 0}
        for _ in range(3):
            try:
                with self.ctx.db.begin() as conn:
                    conn.execute(S.kv.insert().values(key=self.LOCK_KEY, value=doc, updated_at=now_iso()))
                return
            except IntegrityError:
                pass
            cur = self.reencryption_state()
            if cur is None:
                continue
            if cur.get("state") == "running" and not cur.get("stale"):
                raise Conflict(f"a re-encryption is already running (operation {cur.get('operation_id')} on {cur.get('node')})",
                               details={"operation_id": cur.get("operation_id")}, remediation="wait for it, or re-run when it has finished")
            with self.ctx.db.connect() as conn:
                row = conn.execute(select(S.kv.c.updated_at).where(S.kv.c.key == self.LOCK_KEY)).first()
            if row is None:
                continue
            with self.ctx.db.begin() as conn:  # compare-and-swap on the heartbeat timestamp we observed
                if conn.execute(update(S.kv).where(S.kv.c.key == self.LOCK_KEY, S.kv.c.updated_at == row[0]).values(value=doc, updated_at=now_iso())).rowcount:
                    return
        raise Conflict("could not claim the re-encryption (another node is starting it); retry")

    def _beat(self, **fields: Any) -> None:
        from sqlalchemy import update

        # read-modify-write of one JSON document written by two threads (the operation body and the request that records the operation id): serialized, or one of
        # them overwrites the other's fields (a finished operation whose status still said "pending")
        with self.ctx.db.begin_serialized("olcf.reencryption.state") as conn:
            row = conn.execute(select(S.kv.c.value).where(S.kv.c.key == self.LOCK_KEY)).first()
            doc = {**(dict(row[0]) if row else {}), **fields}
            conn.execute(update(S.kv).where(S.kv.c.key == self.LOCK_KEY).values(value=doc, updated_at=now_iso()))

    def reencrypt(self, principal, *, wait: bool) -> dict[str, Any]:  # noqa: ANN001
        """Start (or resume) the re-encryption of every ``file`` secret under the primary key as a tracked operation."""
        pending = self.ctx.secrets.pending_reencryption()
        # claim the lock before the operation exists, so a conflict surfaces as 409 to the caller; the operation id is recorded once it exists
        self._claim_lock("pending")

        def body(progress) -> dict[str, Any]:  # noqa: ANN001
            total = max(pending, 1)
            last_beat = [0.0]
            self._beat(total=pending, node=self.ctx.node_id)

            def on_progress(done: int, skipped: int, failed: int) -> None:
                progress(int(done * 99 / total), f"re-encrypted {done} of {pending} secrets")
                if time.monotonic() - last_beat[0] > 2:
                    last_beat[0] = time.monotonic()
                    self._beat(done=done, skipped=skipped, failed=failed)

            try:
                res = self.ctx.secrets.reencrypt_all(on_progress=on_progress)
            except Exception as exc:
                self._beat(state="failed", finished_at=now_iso(), error=str(exc)[:300])
                self._audit(principal, "master_key.reencrypted", "master-key", outcome="failure", error=str(exc)[:200])
                raise
            ok = not res["failed"] and res["remaining"] == 0
            self._beat(state="succeeded" if ok else "failed", finished_at=now_iso(), done=res["reencrypted"], skipped=res["skipped"], failed=len(res["failed"]),
                       remaining=res["remaining"])
            self._audit(principal, "master_key.reencrypted", "master-key", outcome="success" if ok else "failure", reencrypted=res["reencrypted"],
                        skipped=res["skipped"], failed=res["failed"][:20], remaining=res["remaining"])
            if not ok:
                raise Conflict(f"{len(res['failed'])} secret(s) could not be re-encrypted and {res['remaining']} remain under older keys",
                               details={"failed": res["failed"][:50], "failures": res.get("failures", {}), "remaining": res["remaining"]},
                               remediation="the key that sealed them is not available on this node; restore it (OLCF_MASTER_KEY / master.key) and re-run the re-encryption")
            return {**res, "primary": self.master.current_id}

        try:
            out = run_operation(self.ctx, "security.reencrypt", principal, body, message=f"re-encrypting {pending} secret(s)", wait=wait)
        except Exception:
            self._beat(state="failed", finished_at=now_iso(), error="could not start the operation")
            raise
        self._beat(operation_id=out["operation_id"])
        out["pending"] = pending
        return out

    def drop_old_master(self, principal) -> dict[str, Any]:  # noqa: ANN001
        usage = self.ctx.secrets.key_usage()
        primary = self.master.current_id
        blockers = self._master_drop_blockers(usage, primary)
        if blockers:
            raise Conflict("old master keys are still in use", details={"blockers": blockers}, remediation="; ".join(blockers))
        dropped = self.master.drop_old_ring(principal.id)
        # a straggler node that still sealed under an old key just before it adopted the rotation would leave an unreadable row: look once more
        usage_after = self.ctx.secrets.key_usage()
        orphans = sum(n for kid, n in usage_after.items() if kid not in ("-", *self.master.keys))
        self._audit(principal, "master_key.dropped", "master-key", dropped=dropped, orphaned_secrets=orphans)
        out = {"kind": "master-key", "action": "drop-old", "dropped": dropped, "primary": self.master.current_id}
        if orphans:
            out["warning"] = f"{orphans} secret(s) are sealed under a key that was just dropped (written by a node that had not yet adopted the rotation); roll back to restore access"
        return out

    def rollback(self, kind: str, principal, *, to: str | None, reencrypt: bool, wait: bool) -> dict[str, Any]:  # noqa: ANN001
        target = self._ring_of(kind)
        ring = target.ring if kind == "master-key" else self.jwt.ring
        if ring is None or ring.db is None:
            raise BadRequest("the key ring is not available on this node")
        kid = to
        if not kid:
            last = next((h for h in reversed(ring.history()) if h.get("action") in ("rotate", "rollback") and h.get("previous")), None)
            kid = last.get("previous") if last else None
        if not kid:
            raise Conflict("nothing to roll back to: no key operation has been recorded", remediation="pass the key id with --to")
        previous = target.promote(kid, principal.id)
        self._audit(principal, "master_key.rolled_back" if kind == "master-key" else "jwt_key.rolled_back", kind, primary=kid, demoted=previous)
        out: dict[str, Any] = {"kind": kind, "action": "rollback", "primary": kid, "demoted": previous}
        if kind == "master-key" and reencrypt:
            out.update(self.reencrypt(principal, wait=wait))
        return out

    def rewrap(self, kind: str, principal) -> dict[str, Any]:  # noqa: ANN001
        """Re-wrap the key rings under the newest root key (both rings: the root key wraps them both)."""
        n = self.master.rewrap_ring(principal.id) if self.master.ring.doc else 0
        j = 0
        if self.jwt.ring is not None and self.jwt.ring.doc:
            j = self.jwt.ring.rewrap(principal.id)
        if not n and not j and not (self.master.ring.doc or (self.jwt.ring and self.jwt.ring.doc)):
            raise Conflict("there is no key ring yet: nothing to re-wrap", remediation="rotate a key first, or simply replace the root key if nothing was rotated through the API")
        self._audit(principal, "keys.rewrapped", kind, master_keys=n, jwt_keys=j, root_key=self.master.backup_id)
        return {"kind": kind, "action": "rewrap", "rewrapped": {"master-key": n, "jwt-signing-key": j}, "root_key": self.master.backup_id}

    # ------------------------------------------------------------------ jwt
    def rotate_jwt(self, principal) -> dict[str, Any]:  # noqa: ANN001
        new, previous = self.jwt.rotate_ring(principal.id)
        self._audit(principal, "jwt_key.rotated", "jwt-signing-key", new_key=new, previous_key=previous)
        return {"kind": "jwt-signing-key", "action": "start", "new_key": new, "previous_key": previous,
                "propagation": f"every control-plane node adopts the new key within {refresh_seconds():g} s; tokens signed with the previous key stay valid until they expire "
                               f"(at most {self.max_token_lifetime()} s)", "drop_old_after_seconds": self.max_token_lifetime()}

    def drop_old_jwt(self, principal, *, force: bool) -> dict[str, Any]:  # noqa: ANN001
        entries = self.jwt.key_entries()
        blockers, _eligible = self._jwt_drop_blockers(entries)
        if blockers and not force:
            raise Conflict("the previous signing key may still be verifying live tokens", details={"blockers": blockers}, remediation="wait, or pass force=true to sign those users out")
        dropped = self.jwt.drop_old_ring(principal.id)
        self._audit(principal, "jwt_key.dropped", "jwt-signing-key", dropped=dropped, forced=bool(blockers and force))
        return {"kind": "jwt-signing-key", "action": "drop-old", "dropped": dropped, "primary": self.jwt.current_kid, "forced": bool(blockers and force)}


def service(ctx: AppContext) -> KeyRotationService:
    svc = ctx.maybe_service("keys")
    if svc is None:
        svc = KeyRotationService(ctx)
        ctx.register_service("keys", svc)
    return svc


# ===================================================================== routes
@router.get("/admin/security/keys")
def key_status(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.keys")
    return service(ctx).status()


@router.post("/admin/security/rotate/{kind}")
def rotate(kind: Literal["master-key", "jwt-signing-key"], body: RotateBody, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.keys")
    svc = service(ctx)
    a = body.action
    try:
        if kind == "master-key":
            if a == "start":
                return svc.rotate_master(principal, reencrypt=body.reencrypt, wait=wait)
            if a == "reencrypt":
                return {"kind": kind, "action": a, **svc.reencrypt(principal, wait=wait)}
            if a == "drop-old":
                return svc.drop_old_master(principal)
        else:
            if a == "start":
                return svc.rotate_jwt(principal)
            if a == "reencrypt":
                raise BadRequest("signing keys hold no stored ciphertext: there is nothing to re-encrypt")
            if a == "drop-old":
                return svc.drop_old_jwt(principal, force=body.force)
        if a == "rollback":
            return svc.rollback(kind, principal, to=body.to, reencrypt=body.reencrypt, wait=wait)
        return svc.rewrap(kind, principal)
    except (Conflict, BadRequest, NotFound) as exc:
        svc._audit(principal, f"{kind.replace('-', '_')}.{a.replace('-', '_')}_refused", kind, outcome="failure", error=exc.message[:200])
        raise


# ===================================================================== health
def register_health(ctx: AppContext) -> None:
    def quick() -> ComponentHealth:
        svc = service(ctx)
        st = svc.status()
        problems: list[str] = []
        for part in ("master_key", "jwt_signing_key"):
            d = st[part]
            problems += [f"{part}: {e}" for e in d["errors"]]
            for e in d["keys"]:
                if e["stage"] == "overlap" and e["source"] == "ring" and (e.get("age_days") or 0) > STUCK_AFTER_DAYS:
                    problems.append(f"{part}: rotation started {e['age_days']:.0f} days ago was never finished (key {e['kid']} still in overlap): run drop-old")
        r = st["master_key"]["reencryption"]
        if r and r.get("state") == "running" and r.get("stale"):
            problems.append("a re-encryption stopped without finishing: re-run it (`olcf security rotate master-key --action reencrypt`)")
        if r and r.get("state") == "failed":
            problems.append(f"the last re-encryption failed ({r.get('failed', 0)} secret(s) unreadable)")
        labels = {"master_primary": st["master_key"]["primary"], "jwt_primary": st["jwt_signing_key"]["primary"], "ring_versions": [st["master_key"]["ring_version"], st["jwt_signing_key"]["ring_version"]]}
        if problems:
            return ComponentHealth("key-rotation", Status.DEGRADED, "; ".join(problems)[:400], category="security", labels=labels)
        return ComponentHealth("key-rotation", Status.OK, f"master key {labels['master_primary']}, signing key {labels['jwt_primary']}; no rotation pending", category="security", labels=labels)

    ctx.health.register(HealthCheck("key-rotation", "security", "Master / JWT signing key rings and rotation progress", quick, None, ttl=30))
