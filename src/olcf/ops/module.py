# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Operations module (``olcf.ops.module``): backup/restore/DR, updates, diagnostics + support bundle, retention and housekeeping.

Services registered: ``backup``, ``updates``, ``diagnostics``, ``retention``. Health checks: ``backup``, ``diagnostics``.
Background work (backup schedule, restore-test schedule, retention loop, pending-update bookkeeping) runs on the **leader only**.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.health import ComponentHealth, HealthCheck, Status

log = logging.getLogger("olcf.ops")


class OpsModule(Module):
    """Module that registers the backup, diagnostics, retention and update routes and services."""
    name = "ops"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.ops import backup_api, diagnostics_api, retention_api, updates_api

        self.ctx = ctx
        self.routers = [(m.router, "") for m in (backup_api, updates_api, diagnostics_api, retention_api)]
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-ops", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        interval = float(os.environ.get("OLCF_OPS_TICK_SECONDS", "30"))
        while not self._stop.wait(interval):
            self.tick()

    def tick(self) -> dict[str, Any]:
        """One leader-only housekeeping pass (public so tests and the HA test can drive it deterministically)."""
        out: dict[str, Any] = {}
        if not self.ctx.leader.is_leader():
            return out
        for name in ("backup", "retention", "updates"):
            svc = self.ctx.maybe_service(name)
            if svc is None or not hasattr(svc, "tick"):
                continue
            try:
                res = svc.tick(datetime.now(UTC))
                if res:
                    out[name] = res
            except Exception:  # noqa: BLE001
                log.exception("ops %s tick failed", name)
        return out


def _backup_health(ctx: AppContext, svc: Any) -> tuple[Status, str, dict[str, Any]]:
    h = svc.health()
    cfg = svc.get_config()
    labels = {k: v for k, v in h.items() if v is not None}
    if h.get("last_restore_test") and not h["last_restore_test"]["ok"]:
        return Status.DEGRADED, "the last restore-test FAILED - backups may not be restorable", labels
    if cfg["schedule"]["enabled"]:
        if h["last_success"] is None:
            return Status.DEGRADED, "scheduled backups are enabled but none has succeeded yet", labels
        if h["last_failed"]:
            return Status.DEGRADED, "the most recent backup attempt failed", labels
        if h.get("age_hours", 0) > 48:
            return Status.DEGRADED, f"the last successful backup is {h['age_hours']} h old", labels
        return Status.OK, f"last successful backup {h['age_hours']} h ago ({h['last_id']})", labels
    if h["last_success"] is None:
        return Status.OK, "no backup has been taken yet (scheduled backups are disabled)", labels
    return Status.OK, f"scheduled backups disabled; last backup {h.get('age_hours')} h ago", labels


def _backup_deep(ctx: AppContext, svc: Any) -> ComponentHealth:
    """Active test: dump the live database to a scratch file, verify it end to end and test every enabled target."""
    import tempfile
    from pathlib import Path

    from olcf.ops import backup_core as core
    from olcf.ops.crypto import KeySpec

    steps: list[dict[str, Any]] = []
    t0 = time.perf_counter()
    status = Status.OK
    tmp = ctx.settings.path("tmp")
    tmp.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=tmp) as td:
        try:
            spec = KeySpec(None, ctx.secrets.master)
            res = core.write_backup(ctx.db, Path(td) / "probe.olcfbak", spec, meta={"backup_id": "health-probe", "kind": "probe"})
            steps.append({"name": "dump", "ok": True, "detail": f"{res.rows} rows, {res.size_bytes} bytes, {len(res.tables)} tables"})
            val = core.validate_backup(Path(td) / "probe.olcfbak", spec)
            steps.append({"name": "verify", "ok": val.ok, "detail": "; ".join(val.errors[:3]) or "decrypts, checksums match, schema compatible"})
            if not val.ok:
                status = Status.DOWN
        except Exception as exc:  # noqa: BLE001
            steps.append({"name": "dump", "ok": False, "detail": f"{type(exc).__name__}: {exc}"[:300]})
            status = Status.DOWN
    for tgt in svc.list_targets():
        if not tgt["enabled"]:
            continue
        res = svc.test_target(tgt["name"])
        steps.append({"name": f"target:{tgt['name']}", "ok": res["ok"], "detail": res.get("detail", "")})
        if not res["ok"] and status is Status.OK:
            status = Status.DEGRADED
    stat, detail, labels = _backup_health(ctx, svc)
    if stat.rank > status.rank:
        status = stat
    return ComponentHealth("backup", status, detail if status is stat else next((s["detail"] for s in steps if not s["ok"]), detail), category="operations",
                           labels=labels, steps=steps, mode="deep", latency_ms=(time.perf_counter() - t0) * 1000)


def install(ctx: AppContext) -> OpsModule:
    from olcf.ops.backup import BackupService

    backup = BackupService(ctx)
    ctx.register_service("backup", backup)

    def quick() -> ComponentHealth:
        st, detail, labels = _backup_health(ctx, backup)
        return ComponentHealth("backup", st, detail, category="operations", labels=labels)

    ctx.health.register(HealthCheck("backup", "operations", "Scheduled backups, targets and restore-test freshness", quick, lambda: _backup_deep(ctx, backup), ttl=30))
    from olcf.ops.diagnostics import install_diagnostics
    from olcf.ops.retention import install_retention
    from olcf.ops.support_bundle import install_support
    from olcf.ops.updates import install_updates

    for installer in (install_updates, install_diagnostics, install_support, install_retention):
        installer(ctx)
    return OpsModule(ctx)
