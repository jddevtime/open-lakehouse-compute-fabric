# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Operations wrappers: backups and restores, data retention, diagnostics and support bundles, software updates.

Same conventions as :mod:`olcf.sdk._resources`.  All of these need administrator permissions (``admin.backup``, ``admin.update``, ...).
Methods that start long-running work accept ``wait=True`` to let the server finish before it answers; otherwise they return an operation
document that :meth:`olcf.sdk.client.AdminApi.wait_operation` can follow.  Secrets (passphrases, target credentials) are write-only: the
server never returns them.
"""

from __future__ import annotations

import builtins
from typing import Any

from olcf.sdk._resources import JSON, Body, _Res


def _clean(**fields: Any) -> dict[str, Any]:
    """Drop ``None`` values so the server's own defaults apply."""
    return {k: v for k, v in fields.items() if v is not None}


class Backups(_Res):
    """Backups: create, list, validate, test-restore, restore, import / download, targets (local, S3, SFTP, lakehouse) and the schedule."""

    def list(self, *, limit: int = 100) -> JSON:
        return self._call("GET", "/admin/backup", params={"limit": limit})

    def start(self, *, include_keys: bool | None = None, passphrase: str | None = None, targets: builtins.list[str] | None = None, wait: bool = False) -> JSON:
        """Start a backup.  ``include_keys`` adds the CA / signing / master keys (needs a passphrase)."""
        return self._call("POST", "/admin/backup", params={"wait": wait or None}, json=_clean(include_keys=include_keys, passphrase=passphrase, targets=targets))

    def get(self, backup_id: str) -> JSON:
        return self._call("GET", "/admin/backup/{bid}", path={"bid": backup_id})

    def delete(self, backup_id: str, *, everywhere: bool = True) -> JSON:
        return self._call("DELETE", "/admin/backup/{bid}", path={"bid": backup_id}, params={"everywhere": everywhere})

    def download(self, backup_id: str) -> bytes:
        return self._call("GET", "/admin/backup/{bid}/download", path={"bid": backup_id}, as_bytes=True)

    def import_backup(self, data: bytes, *, filename: str = "upload.olcfbak") -> JSON:
        return self._call("POST", "/admin/backup/import", files={"file": (filename, data, "application/octet-stream")})

    def prerequisites(self, backup_id: str) -> JSON:
        """What a restore of this backup needs (keys, passphrase, versions) and whether it is present."""
        return self._call("GET", "/admin/backup/{bid}/prerequisites", path={"bid": backup_id})

    def validate(self, backup_id: str, *, passphrase: str | None = None, wait: bool = False) -> JSON:
        return self._call("POST", "/admin/backup/{bid}/validate", path={"bid": backup_id}, params={"wait": wait or None}, json=_clean(passphrase=passphrase))

    def restore_test(self, backup_id: str, *, passphrase: str | None = None, wait: bool = False) -> JSON:
        """Restore into a scratch location and verify it, without touching the live installation."""
        return self._call("POST", "/admin/backup/{bid}/restore-test", path={"bid": backup_id}, params={"wait": wait or None}, json=_clean(passphrase=passphrase))

    def restore(self, backup_id: str, *, confirm: str, passphrase: str | None = None, restore_keys: bool = False, wait: bool = False) -> JSON:
        """Restore over the live installation.  ``confirm`` must be the phrase the server asks for (see :meth:`prerequisites`)."""
        return self._call("POST", "/admin/backup/{bid}/restore", path={"bid": backup_id}, params={"wait": wait or None},
                          json=_clean(confirm=confirm, passphrase=passphrase, restore_keys=restore_keys))

    def run_retention(self, *, dry_run: bool = True) -> JSON:
        """Apply the backup retention policy (``dry_run`` only reports what would be deleted)."""
        return self._call("POST", "/admin/backup/retention", params={"dry_run": dry_run})

    # ---- schedule / defaults
    def config(self) -> JSON:
        return self._call("GET", "/admin/backup/config")

    def set_config(self, *, schedule: Body | None = None, retention: Body | None = None, restore_test: Body | None = None, include_keys: bool | None = None,
                   targets: builtins.list[str] | None = None, passphrase: str | None = None) -> JSON:
        return self._call("PUT", "/admin/backup/config", json=_clean(schedule=schedule, retention=retention, restore_test=restore_test, include_keys=include_keys,
                                                                       targets=targets, passphrase=passphrase))

    # ---- targets
    def list_targets(self) -> JSON:
        return self._call("GET", "/admin/backup/targets")

    def create_target(self, name: str, type: str, *, config: Body | None = None, secret: Body | str | None = None, secret_id: str | None = None,  # noqa: A002
                      enabled: bool = True, retention: Body | None = None) -> JSON:
        return self._call("POST", "/admin/backup/targets", json={"name": name, "type": type, "config": dict(config or {}), "secret": secret, "secret_id": secret_id,
                                                                   "enabled": enabled, "retention": dict(retention or {})})

    def get_target(self, ref: str) -> JSON:
        return self._call("GET", "/admin/backup/targets/{ref}", path={"ref": ref})

    def update_target(self, ref: str, **fields: Any) -> JSON:
        """Change ``config``, ``secret``, ``enabled`` or ``retention`` of a target."""
        return self._call("PATCH", "/admin/backup/targets/{ref}", path={"ref": ref}, json=fields)

    def delete_target(self, ref: str) -> None:
        return self._call("DELETE", "/admin/backup/targets/{ref}", path={"ref": ref})

    def test_target(self, ref: str) -> JSON:
        return self._call("POST", "/admin/backup/targets/{ref}/test", path={"ref": ref})

    def target_files(self, ref: str) -> JSON:
        return self._call("GET", "/admin/backup/targets/{ref}/files", path={"ref": ref})


class Retention(_Res):
    """Data retention (events, audit, metrics, job history and logs, recycle bin, expired tokens ...)."""

    def status(self) -> JSON:
        return self._call("GET", "/admin/retention")

    def run(self, *, dry_run: bool = True, only: builtins.list[str] | None = None) -> JSON:
        """Run the retention steps (all, or only the named ones).  ``dry_run`` reports counts without deleting."""
        return self._call("POST", "/admin/retention/run", json=_clean(dry_run=dry_run, only=only))

    def runs(self, *, limit: int = 20) -> JSON:
        return self._call("GET", "/admin/retention/runs", params={"limit": limit})

    def get_run(self, run_id: str) -> JSON:
        return self._call("GET", "/admin/retention/runs/{rid}", path={"rid": run_id})

    def audit(self) -> JSON:
        """Retention of the tamper-evident audit log (what may be pruned without breaking the hash chain)."""
        return self._call("GET", "/admin/retention/audit")


class Diagnostics(_Res):
    """Self-diagnostics and sanitized, leak-scanned support bundles."""

    def run(self, *, only: builtins.list[str] | None = None, spark: bool = False, worker_probes: bool = False, network_time: bool = False, deep_health: bool = True) -> JSON:
        return self._call("POST", "/admin/diagnostics", json=_clean(only=only, spark=spark, worker_probes=worker_probes, network_time=network_time, deep_health=deep_health))

    def last(self) -> JSON:
        """The result of the most recent diagnostics run; before the first run ``last_run`` is ``None`` and ``checks`` is empty (no error)."""
        return self._call("GET", "/admin/diagnostics")

    def list_bundles(self) -> JSON:
        return self._call("GET", "/admin/support-bundle")

    def create_bundle(self, *, sanitize: bool = True, include_logs: bool = True, include_config: bool = True, include_diagnostics: bool = True, include_health: bool = True,
                      include_audit: bool = False, audit_limit: int = 500, include_jobs: bool = False, job_limit: int = 100, include_worker_diagnostics: bool = False,
                      log_tail_kb: int = 1024) -> JSON:
        """Create a support bundle.  Audit, job and per-worker sections are opt-in; sanitising is on by default."""
        return self._call("POST", "/admin/support-bundle", json={
            "sanitize": sanitize, "include_logs": include_logs, "include_config": include_config, "include_diagnostics": include_diagnostics,
            "include_health": include_health, "include_audit": include_audit, "audit_limit": audit_limit, "include_jobs": include_jobs, "job_limit": job_limit,
            "include_worker_diagnostics": include_worker_diagnostics, "log_tail_kb": log_tail_kb})

    def get_bundle(self, bundle_id: str) -> JSON:
        return self._call("GET", "/admin/support-bundle/{bid}", path={"bid": bundle_id})

    def download_bundle(self, bundle_id: str) -> bytes:
        return self._call("GET", "/admin/support-bundle/{bid}/download", path={"bid": bundle_id}, as_bytes=True)

    def delete_bundle(self, bundle_id: str) -> None:
        return self._call("DELETE", "/admin/support-bundle/{bid}", path={"bid": bundle_id})


class Updates(_Res):
    """Software updates of the control plane: check a signed feed or import a bundle, stage, preflight, apply, roll back."""

    def status(self) -> JSON:
        return self._call("GET", "/admin/updates")

    def config(self) -> JSON:
        return self._call("GET", "/admin/updates/config")

    def set_config(self, **fields: Any) -> JSON:
        """Change ``channel``, ``feed_url``, ``proxy``, ``ca_bundle``, ``maintenance_windows``, ``auto_check`` ... (unset fields stay as they are)."""
        return self._call("PUT", "/admin/updates/config", json=_clean(**fields))

    def check(self, *, channel: str | None = None) -> JSON:
        return self._call("POST", "/admin/updates/check", json=_clean(channel=channel))

    def import_bundle(self, data: bytes, *, filename: str = "update.olcfupd") -> JSON:
        """Offline update: upload a signed update bundle."""
        return self._call("POST", "/admin/updates/import", files={"file": (filename, data, "application/octet-stream")})

    def get(self, update_id: str) -> JSON:
        return self._call("GET", "/admin/updates/{uid}", path={"uid": update_id})

    def stage(self, update_id: str, *, wait: bool = False) -> JSON:
        return self._call("POST", "/admin/updates/{uid}/stage", path={"uid": update_id}, params={"wait": wait or None})

    def preflight(self, update_id: str, *, wait: bool = False) -> JSON:
        return self._call("POST", "/admin/updates/{uid}/preflight", path={"uid": update_id}, params={"wait": wait or None})

    def apply(self, update_id: str, *, confirm: str, override_window: bool = False, execute: bool = False, wait: bool = False) -> JSON:
        """Apply a staged update.  ``execute`` also starts the hand-off to the upgrader; ``confirm`` is the phrase the server expects."""
        return self._call("POST", "/admin/updates/{uid}/apply", path={"uid": update_id}, params={"wait": wait or None},
                          json={"confirm": confirm, "override_window": override_window, "execute": execute})

    def cancel(self, update_id: str) -> JSON:
        return self._call("POST", "/admin/updates/{uid}/cancel", path={"uid": update_id})

    def rollback(self, update_id: str) -> JSON:
        return self._call("POST", "/admin/updates/{uid}/rollback", path={"uid": update_id})

    def maintenance(self, on: bool) -> JSON:
        """Drain (``True``) or resume (``False``) the node: new jobs wait while running ones finish."""
        return self._call("POST", "/admin/updates/maintenance", json={"on": on})

    # ---- trusted update-signing keys
    def trust_list(self) -> JSON:
        return self._call("GET", "/admin/updates/trust")

    def trust_add(self, name: str, public_key: str) -> JSON:
        """Trust an Ed25519 update-signing key (base64 of the raw 32-byte public key)."""
        return self._call("POST", "/admin/updates/trust", json={"name": name, "public_key": public_key})

    def trust_revoke(self, key_id: str) -> None:
        return self._call("DELETE", "/admin/updates/trust/{keyid}", path={"keyid": key_id})


class Security(_Res):
    """Credential lifecycle (permission ``admin.keys``, role SecurityAdmin): key rings of the master key and the JWT signing key, staged rotation.

    ``keys()`` lists key ids, stages, ages, sources and usage - never key material.  ``rotate_*`` run one stage: ``start`` (new primary key, previous one
    stays as an overlap key; for the master key it also launches the resumable re-encryption), ``reencrypt``, ``drop-old``, ``rollback`` and ``rewrap``.
    """

    def keys(self) -> JSON:
        return self._call("GET", "/admin/security/keys")

    def rotate(self, kind: str, action: str = "start", *, reencrypt: bool | None = None, force: bool | None = None, to: str | None = None, wait: bool = False) -> JSON:
        """Run one rotation stage on ``kind`` (``master-key`` or ``jwt-signing-key``).  With ``wait`` the server finishes the re-encryption before it answers."""
        return self._call("POST", "/admin/security/rotate/{kind}", path={"kind": kind}, params={"wait": wait or None},
                          json=_clean(action=action, reencrypt=reencrypt, force=force, to=to))

    def rotate_master_key(self, action: str = "start", *, reencrypt: bool | None = None, to: str | None = None, wait: bool = False) -> JSON:
        return self.rotate("master-key", action, reencrypt=reencrypt, to=to, wait=wait)

    def rotate_jwt_signing_key(self, action: str = "start", *, force: bool | None = None, to: str | None = None) -> JSON:
        return self.rotate("jwt-signing-key", action, force=force, to=to)

