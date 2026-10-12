# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Persistence and administration of discovery sources: create / update / delete with credential handling, the cross-replica scan lease and the cancel flag.

Credentials are write-only. ``create`` / ``update`` accept ``credentials`` (metadata identity) and ``sampling_credentials`` (sampling identity), store them with the platform
secret store and keep only the ``sec_`` id in the row (``secret_ref`` / ``config.sampling_identity.secret_id``). Every read model produced here (:meth:`SourceStore.public`) is passed
through the secret redactor, shows ``has_credentials`` instead of any value and never contains the compressed scanner payload.

The scan lease (``lock_*`` columns) is how "one running scan per source" holds across replicas: :meth:`SourceStore.acquire` is a single conditional ``UPDATE``, a heartbeat extends it
while a scan runs, and a lease whose owner stopped (its expiry passed) can be taken over - the abandoned ``running`` scan row is then closed as ``failed``.
"""

from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from sqlalchemy import exists, insert, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, Conflict, NotFound, ValidationFailed
from olcf.core.ids import new_id
from olcf.core.redaction import redact_obj
from olcf.core.timeutil import from_iso, now_iso, to_iso, utcnow
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.intelligence.discovery import identities
from olcf.intelligence.discovery.base import connector_for, registered_kinds
from olcf.intelligence.discovery.schema import SOURCE_KINDS, discovery_sources

log = logging.getLogger("olcf.intelligence.discovery.sources")

DEFAULT_LEASE_SECONDS = 120
LOCAL_SYSTEM_KEY = "local-default"
SCHEDULE_KEYS = ("incremental_seconds", "full_seconds")
_INTERNAL_COLUMNS = ("payload", "state", "lock_scan_id", "lock_holder", "lock_expires_at", "cancel_scan_id")


def holder_id() -> str:
    """Identity of this process and thread for the lease (``host:pid:thread``)."""
    import os

    return f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}"


def clean_schedule(schedule: Any) -> dict[str, int]:
    """Validate a source's schedule: ``{"incremental_seconds": n, "full_seconds": m}``, each 0 (never) or 60..2592000; absent keys use the platform defaults."""
    if schedule in (None, {}):
        return {}
    if not isinstance(schedule, Mapping) or set(schedule) - set(SCHEDULE_KEYS):
        raise ValidationFailed(f"schedule must be an object with only {list(SCHEDULE_KEYS)}")
    out: dict[str, int] = {}
    for k, v in schedule.items():
        if isinstance(v, bool) or not isinstance(v, int) or not (v == 0 or 60 <= v <= 2_592_000):
            raise ValidationFailed(f"schedule.{k} must be 0 (never) or between 60 and 2592000 seconds")
        out[k] = v
    return out


class SourceStore:
    """CRUD, credentials and leases of ``discovery_sources`` rows."""

    def __init__(self, ctx: Any, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> None:
        self.ctx = ctx
        self.lease_seconds = lease_seconds

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def get(self, source_id: str) -> dict[str, Any]:
        """The full row (including internal columns); ``NotFound`` if absent."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(discovery_sources).where(discovery_sources.c.id == source_id))
        if row is None:
            raise NotFound(f"discovery source '{source_id}' does not exist")
        return row

    def find(self, ref: str) -> dict[str, Any]:
        """A source by id or (unique) name."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(discovery_sources).where(discovery_sources.c.id == ref)) or fetch_one(c, select(discovery_sources).where(discovery_sources.c.name == ref))
        if row is None:
            raise NotFound(f"discovery source '{ref}' does not exist")
        return row

    def list(self, *, kind: str | None = None, enabled: bool | None = None) -> list[dict[str, Any]]:
        """All rows, oldest first, optionally one kind / enabled state."""
        stmt = select(discovery_sources).order_by(discovery_sources.c.created_at, discovery_sources.c.id)
        if kind:
            stmt = stmt.where(discovery_sources.c.kind == kind)
        if enabled is not None:
            stmt = stmt.where(discovery_sources.c.enabled.is_(enabled))
        with self.ctx.db.connect() as c:
            return fetch_all(c, stmt)

    def public(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """The API form of a source: configuration with references only, never a credential or the stored payload; ``running`` reflects a live lease."""
        cfg = dict(row.get("config") or {})
        meta_block = cfg.get("metadata_identity") or {}
        out = {
            "id": row["id"], "name": row["name"], "kind": row["kind"], "enabled": bool(row["enabled"]), "system": bool(row.get("system_key")), "system_key": row.get("system_key"),
            "config": redact_obj(cfg), "schedule": dict(row.get("schedule") or {}),
            "has_credentials": bool(row.get("secret_ref") or meta_block.get("secret_id") or meta_block.get("connection_id")),
            "has_sampling_identity": bool(cfg.get("sampling_identity")),
            "last_scan_id": row.get("last_scan_id"), "last_status": row.get("last_status"), "last_scan_at": row.get("last_scan_at"), "last_full_at": row.get("last_full_at"),
            "last_incremental_at": row.get("last_incremental_at"), "running": self.is_leased(row), "version": row["version"], "etag": row["etag"], "created_by": row.get("created_by"),
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }
        if row["kind"] == "scanner_import":
            out["import"] = {"stored": bool(row.get("payload")), "bytes": len(row["payload"]) if row.get("payload") else 0, "sha256": row.get("payload_sha")}
        return out

    @staticmethod
    def is_leased(row: Mapping[str, Any]) -> bool:
        """True while a live lease is held (its expiry has not passed)."""
        exp = row.get("lock_expires_at")
        return bool(row.get("lock_scan_id") and exp and exp > now_iso())

    # ------------------------------------------------------------------------------------------------------------------------ create
    def create(self, principal: Any, *, name: str, kind: str, config: Mapping[str, Any] | None = None, schedule: Mapping[str, Any] | None = None, enabled: bool = True,
               credentials: Mapping[str, Any] | None = None, sampling_credentials: Mapping[str, Any] | None = None, system_key: str | None = None) -> dict[str, Any]:
        """Create a source. Validates the kind, the configuration (connector-specific), identity references and endpoint overrides, stores any credentials as secrets and audits
        ``intelligence.source_created``. Returns the public view."""
        if not isinstance(name, str) or not name.strip() or len(name) > 128:
            raise ValidationFailed("name must be 1 to 128 characters")
        if kind not in SOURCE_KINDS or kind not in registered_kinds():
            raise BadRequest(f"unknown source kind '{kind}'", details={"kinds": registered_kinds()})
        cfg = self.validate_config(principal, kind, config or {})
        created: list[str] = []
        try:
            secret_ref = None
            if credentials is not None:
                if kind != "fabric" or "connection_id" in (cfg.get("metadata_identity") or {}):
                    raise ValidationFailed("credentials apply only to a Fabric source whose metadata_identity has tenant_id and client_id (not connection_id)")
                secret_ref = identities.store_credentials(self.ctx, principal, name.strip(), "metadata", credentials)
                created.append(secret_ref)
            if sampling_credentials is not None:
                if kind != "fabric" or not cfg.get("sampling_identity") or "connection_id" in cfg["sampling_identity"]:
                    raise ValidationFailed("sampling_credentials need a Fabric source with a sampling_identity that has tenant_id and client_id (not connection_id)")
                sid = identities.store_credentials(self.ctx, principal, name.strip(), "sampling", sampling_credentials)
                created.append(sid)
                cfg["sampling_identity"] = {**cfg["sampling_identity"], "secret_id": sid, "owned": True}
            if kind == "fabric":
                self._require_credentials(cfg, secret_ref)
            now = now_iso()
            new_row_id = new_id("src")
            row: dict[str, Any] = {"id": new_row_id, "name": name.strip(), "kind": kind, "enabled": bool(enabled), "system_key": system_key, "config": cfg, "schedule": clean_schedule(schedule),
                   "secret_ref": secret_ref, "state": {}, "version": 1, "etag": new_etag(), "created_by": getattr(principal, "id", None), "created_at": now, "updated_at": now}
            try:
                with self.ctx.db.begin() as c:
                    c.execute(insert(discovery_sources).values(**row))
            except IntegrityError:
                raise Conflict(f"a discovery source named '{name}' already exists", remediation="choose another name") from None
        except Exception:
            for sid in created:
                identities.delete_secret(self.ctx, sid)
            raise
        self.ctx.audit.record(principal, "intelligence.source_created", resource_type="discovery_source", resource_id=row["id"],
                              details={"name": row["name"], "kind": kind, "enabled": row["enabled"], "has_secret": bool(secret_ref)})
        return self.public(self.get(new_row_id))

    def validate_config(self, principal: Any, kind: str, config: Mapping[str, Any]) -> dict[str, Any]:
        """Connector-level validation plus the checks that need the platform: identity references exist and the caller may use them, endpoint overrides are allowed."""
        connector = connector_for(kind)
        cleaned = connector.validate_config({k: v for k, v in config.items()})
        if kind == "fabric":
            for name in identities.IDENTITY_NAMES:
                block = dict(cleaned.get(name) or {})
                if not block:
                    continue
                block.pop("owned", None)                       # set by the service only, never by a caller
                cleaned[name] = identities.validate_identity_block(self.ctx, principal, block, name=name)
            net = identities.validate_network(principal, cleaned.get("network"), self.ctx)
            if net:
                cleaned["network"] = net
            else:
                cleaned.pop("network", None)
        return dict(cleaned)

    @staticmethod
    def _require_credentials(cfg: Mapping[str, Any], secret_ref: str | None) -> None:
        block = cfg.get("metadata_identity") or {}
        if not (block.get("connection_id") or block.get("secret_id") or secret_ref):
            raise ValidationFailed("a Fabric source needs credentials for its metadata identity", remediation="send credentials ({client_secret} or {private_key_pem, certificate_pem}) or use metadata_identity.connection_id")

    # ------------------------------------------------------------------------------------------------------------------------ update
    def update(self, principal: Any, source_id: str, changes: Mapping[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a source. ``config`` is merged key by key (``null`` removes a key); ``credentials`` / ``sampling_credentials`` replace the stored secret (the old one is deleted).
        Audits ``intelligence.source_updated`` with the names of the changed fields only."""
        from olcf.api.deps import check_if_match

        row = self.get(source_id)
        check_if_match(if_match, row["etag"])
        unknown = sorted(set(changes) - {"name", "enabled", "config", "schedule", "credentials", "sampling_credentials"})
        if unknown:
            raise ValidationFailed(f"cannot change {unknown}; a source's kind is fixed")
        vals: dict[str, Any] = {}
        fields: list[str] = []
        old_secrets: list[str] = []
        new_secrets: list[str] = []
        cfg = dict(row["config"] or {})
        try:
            if "name" in changes and changes["name"] != row["name"]:
                if not isinstance(changes["name"], str) or not changes["name"].strip() or len(changes["name"]) > 128:
                    raise ValidationFailed("name must be 1 to 128 characters")
                vals["name"] = changes["name"].strip()
                fields.append("name")
            if "enabled" in changes:
                vals["enabled"] = bool(changes["enabled"])
                fields.append("enabled")
            if "schedule" in changes:
                vals["schedule"] = clean_schedule(changes["schedule"])
                fields.append("schedule")
            if "config" in changes:
                if not isinstance(changes["config"], Mapping):
                    raise ValidationFailed("config must be an object")
                merged = dict(cfg)
                for k, v in changes["config"].items():
                    if v is None:
                        if k == "sampling_identity" and (merged.get(k) or {}).get("owned") and merged[k].get("secret_id"):
                            old_secrets.append(merged[k]["secret_id"])
                        merged.pop(k, None)
                    else:
                        merged[k] = v
                prior_sampling = cfg.get("sampling_identity") or {}
                cleaned = self.validate_config(principal, row["kind"], merged)
                if "sampling_identity" in cleaned and prior_sampling.get("owned") and cleaned["sampling_identity"].get("secret_id") == prior_sampling.get("secret_id"):
                    cleaned["sampling_identity"]["owned"] = True        # the same owned secret stays owned
                cfg = cleaned
                fields.append("config")
            if "credentials" in changes:
                if row["kind"] != "fabric" or "connection_id" in (cfg.get("metadata_identity") or {}):
                    raise ValidationFailed("credentials apply only to a Fabric source whose metadata_identity has tenant_id and client_id")
                sid = identities.store_credentials(self.ctx, principal, vals.get("name", row["name"]), "metadata", changes["credentials"])
                new_secrets.append(sid)
                if row.get("secret_ref"):
                    old_secrets.append(row["secret_ref"])
                vals["secret_ref"] = sid
                fields.append("credentials")
            if "sampling_credentials" in changes:
                if row["kind"] != "fabric" or not cfg.get("sampling_identity") or "connection_id" in cfg["sampling_identity"]:
                    raise ValidationFailed("sampling_credentials need a sampling_identity with tenant_id and client_id")
                prev = cfg["sampling_identity"]
                sid = identities.store_credentials(self.ctx, principal, vals.get("name", row["name"]), "sampling", changes["sampling_credentials"])
                new_secrets.append(sid)
                if prev.get("owned") and prev.get("secret_id"):
                    old_secrets.append(prev["secret_id"])
                cfg["sampling_identity"] = {**prev, "secret_id": sid, "owned": True}
                if "config" not in fields:
                    fields.append("config")
                fields.append("sampling_credentials")
            if row["kind"] == "fabric" and ("config" in fields or "credentials" in fields):
                self._require_credentials(cfg, vals.get("secret_ref", row.get("secret_ref")))
            if "config" in fields or "sampling_credentials" in fields:
                vals["config"] = cfg
            if not vals:
                return self.public(row)
            vals.update(version=row["version"] + 1, etag=new_etag(), updated_at=now_iso())
            try:
                with self.ctx.db.begin() as c:
                    n = c.execute(update(discovery_sources).where(discovery_sources.c.id == source_id, discovery_sources.c.etag == row["etag"]).values(**vals)).rowcount
                if not n:
                    raise Conflict("the source was changed by someone else; read it again and retry")
            except IntegrityError:
                raise Conflict(f"a discovery source named '{vals.get('name')}' already exists") from None
        except Exception:
            for sid in new_secrets:
                identities.delete_secret(self.ctx, sid)
            raise
        for sid in old_secrets:
            identities.delete_secret(self.ctx, sid)
        self.ctx.audit.record(principal, "intelligence.source_updated", resource_type="discovery_source", resource_id=source_id, details={"fields": sorted(set(fields)), "name": row["name"]})
        return self.public(self.get(source_id))

    # ------------------------------------------------------------------------------------------------------------------------ delete
    def delete(self, principal: Any, source_id: str) -> dict[str, Any]:
        """Delete a source (never the system source, never while a scan runs): its owned secrets are removed; the caller retires its assets afterwards. Audits ``intelligence.source_deleted``."""
        row = self.get(source_id)
        if row.get("system_key"):
            raise Conflict(f"'{row['name']}' is a system source and cannot be deleted", remediation="disable it instead (PATCH enabled=false)")
        if self.is_leased(row):
            raise Conflict(f"a scan of '{row['name']}' is running ({row['lock_scan_id']})", remediation="cancel it first (POST /v1/intelligence/scans/{id}/cancel)")
        with self.ctx.db.begin() as c:
            c.execute(discovery_sources.delete().where(discovery_sources.c.id == source_id))
        owned = [row["secret_ref"]] if row.get("secret_ref") else []
        samp = (row["config"] or {}).get("sampling_identity") or {}
        if samp.get("owned") and samp.get("secret_id"):
            owned.append(samp["secret_id"])
        for sid in owned:
            identities.delete_secret(self.ctx, sid)
        self.ctx.audit.record(principal, "intelligence.source_deleted", resource_type="discovery_source", resource_id=source_id, details={"name": row["name"], "kind": row["kind"]})
        return row

    def ensure_system_source(self, key: str, *, name: str, kind: str, config: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Idempotently create a system source (no principal, no audit: it is part of the installation) and return its row."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(discovery_sources).where(discovery_sources.c.system_key == key))
        if row is not None:
            return row
        now = now_iso()
        values = {"id": new_id("src"), "name": name, "kind": kind, "enabled": True, "system_key": key, "config": dict(config or {}), "schedule": {}, "secret_ref": None, "state": {},
                  "version": 1, "etag": new_etag(), "created_by": None, "created_at": now, "updated_at": now}
        try:
            with self.ctx.db.begin() as c:
                c.execute(insert(discovery_sources).values(**values))
        except IntegrityError:
            pass                                               # another replica created it first
        return self.find_system(key)

    def find_system(self, key: str) -> dict[str, Any]:
        """The system source with this key."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(discovery_sources).where(discovery_sources.c.system_key == key))
        if row is None:
            raise NotFound(f"system source '{key}' does not exist")
        return row

    # ------------------------------------------------------------------------------------------------------------------------ payload (scanner_import)
    def store_payload(self, source_id: str, blob: bytes, sha: str) -> None:
        """Replace the stored scanner document of a ``scanner_import`` source."""
        with self.ctx.db.begin() as c:
            c.execute(update(discovery_sources).where(discovery_sources.c.id == source_id).values(payload=blob, payload_sha=sha, updated_at=now_iso(), etag=new_etag()))

    # ------------------------------------------------------------------------------------------------------------------------ lease
    def acquire(self, source_id: str, token: str, *, holder: str | None = None) -> dict[str, Any] | None:
        """Take the scan lease with ONE conditional UPDATE (the database arbitrates between concurrent callers, on any replica); returns ``None`` on success, or the current
        (live) row if somebody else holds it.

        The lease is free when nobody holds it, when it has expired (takeover) or when the scan it points to has already been closed: the scan row is closed a moment before the
        holder releases the lease, and a caller that sees the finished scan must not be refused for that gap.
        """
        from olcf.intelligence.schema import scans

        now = now_iso()
        exp = to_iso(utcnow() + timedelta(seconds=self.lease_seconds))
        t = discovery_sources.c
        finished = exists().where(scans.c.id == t.lock_scan_id, scans.c.status != "running")
        with self.ctx.db.begin() as c:
            n = c.execute(update(discovery_sources).where(t.id == source_id, (t.lock_scan_id.is_(None)) | (t.lock_expires_at.is_(None)) | (t.lock_expires_at < now) | finished)
                          .values(lock_scan_id=token, lock_holder=holder or holder_id(), lock_expires_at=exp)).rowcount
            if n:
                return None
            return fetch_one(c, select(discovery_sources).where(t.id == source_id))

    def abandoned(self, source_id: str) -> str | None:
        """The ``scn_`` id of a ``running`` scan row left behind by a lease holder that disappeared (call right after a successful :meth:`acquire`)."""
        from olcf.intelligence.schema import scans

        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(scans.c.id).where(scans.c.source_id == source_id, scans.c.status == "running").order_by(scans.c.version.desc()).limit(1))
        return row["id"] if row else None

    def rebind(self, source_id: str, token: str, scan_id: str) -> None:
        """Point the lease at the scan that was opened under it."""
        with self.ctx.db.begin() as c:
            c.execute(update(discovery_sources).where(discovery_sources.c.id == source_id, discovery_sources.c.lock_scan_id == token).values(lock_scan_id=scan_id))

    def heartbeat(self, source_id: str, scan_id: str) -> bool:
        """Extend the lease of a running scan; False if the lease was lost (taken over)."""
        exp = to_iso(utcnow() + timedelta(seconds=self.lease_seconds))
        with self.ctx.db.begin() as c:
            return bool(c.execute(update(discovery_sources).where(discovery_sources.c.id == source_id, discovery_sources.c.lock_scan_id == scan_id).values(lock_expires_at=exp)).rowcount)

    def release(self, source_id: str, *tokens: str) -> None:
        """Release the lease held under any of ``tokens`` (the pre-scan token or the scan id) and clear a pending cancel request for it."""
        t = discovery_sources.c
        with self.ctx.db.begin() as c:
            c.execute(update(discovery_sources).where(t.id == source_id, t.lock_scan_id.in_(list(tokens))).values(lock_scan_id=None, lock_holder=None, lock_expires_at=None, cancel_scan_id=None))

    def request_cancel(self, source_id: str, scan_id: str) -> bool:
        """Ask the scan of this source to stop; True if the source's lease is live (the scan will notice between batches, on any replica), False if nobody holds it."""
        t = discovery_sources.c
        with self.ctx.db.begin() as c:
            return bool(c.execute(update(discovery_sources).where(t.id == source_id, t.lock_expires_at > now_iso()).values(cancel_scan_id=scan_id)).rowcount)

    def cancel_requested(self, source_id: str, scan_id: str) -> bool:
        """True when a cancel request for ``scan_id`` is pending."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(discovery_sources.c.cancel_scan_id).where(discovery_sources.c.id == source_id))
        return bool(row and row["cancel_scan_id"] == scan_id)

    def is_scan_row(self, scan_id: str) -> bool:
        """True when ``scan_id`` names a ``scans`` row (a lease still held under its pre-scan token does not)."""
        from olcf.intelligence.schema import scans

        with self.ctx.db.connect() as c:
            return fetch_one(c, select(scans.c.id).where(scans.c.id == scan_id)) is not None

    def record_result(self, source_id: str, *, scan_id: str, status: str, started_at: str, mode: str, state: dict[str, Any] | None, ok: bool, release: tuple[str, ...] = ()) -> None:
        """Store the outcome on the source row: last scan, last full / incremental start (only for scans that were not failed or cancelled) and, when ``state`` is given, the connector
        checkpoint. ``release`` (the scan id and pre-scan token) also gives up the lease **in the same transaction**, so nobody sees the result without the lease being free."""
        vals: dict[str, Any] = {"last_scan_id": scan_id, "last_status": status, "last_scan_at": now_iso()}
        if ok:
            vals["last_full_at" if mode == "full" else "last_incremental_at"] = started_at
        if state is not None:
            vals["state"] = state
        t = discovery_sources.c
        with self.ctx.db.begin() as c:
            c.execute(update(discovery_sources).where(t.id == source_id).values(**vals))
            if release:
                c.execute(update(discovery_sources).where(t.id == source_id, t.lock_scan_id.in_(list(release)))
                          .values(lock_scan_id=None, lock_holder=None, lock_expires_at=None, cancel_scan_id=None))

    def timestamp(self, iso: str | None) -> Any:
        """Parse a stored timestamp (``None`` stays ``None``)."""
        return from_iso(iso) if iso else None
